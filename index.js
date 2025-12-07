// index.js - Backend Principal com UTMify e Pixels Atualizados - VERSÃO CORRIGIDA
const express = require('express');
const axios = require('axios');
const { Pool } = require('pg');
require('dotenv').config();
const crypto = require('crypto');

const app = express();

// Middleware básico
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// CORS para aceitar requests de qualquer origem
app.use((req, res, next) => {
    const allowedOrigins = [
        '*',
        'http://127.0.0.1:5500',
        'http://localhost:5500',
        'http://localhost:3000',
        'https://utm-ujn8.onrender.com',
        'https://lelelinksbr.shop'
    ];

    const origin = req.headers.origin;
    if (allowedOrigins.includes(origin) || allowedOrigins.includes('*')) {
        res.header('Access-Control-Allow-Origin', origin || '*');
    }

    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.header('Access-Control-Allow-Credentials', 'true');

    // Handle preflight
    if (req.method === 'OPTIONS') {
        return res.status(200).end();
    }

    next();
});

// Adicione logo após as configurações do CORS
app.use((req, res, next) => {
    // Log de todas as requisições
    console.log(`${new Date().toISOString()} ${req.method} ${req.url}`);
    console.log('Headers:', JSON.stringify(req.headers));
    console.log('Query:', JSON.stringify(req.query));
    if (req.method === 'POST') {
        console.log('Body:', JSON.stringify(req.body));
    }
    next();
});

// --- CONFIGURAÇÃO ---
const DATABASE_URL = process.env.DATABASE_URL;
const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_URL = process.env.TELEGRAM_BOT_URL || 'https://t.me/seu_bot';
const UTMIFY_API_KEY = process.env.UTMIFY_API_KEY;

// --- BANCO DE DADOS ---
const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// Função para criar tabelas
async function setupDatabase() {
    console.log('🔧 Configurando banco de dados...');

    // Primeiro, criar as tabelas
    const createTables = [
        // Tabela de cliques
        `CREATE TABLE IF NOT EXISTS clicks (
            id SERIAL PRIMARY KEY,
            click_id TEXT UNIQUE NOT NULL,
            session_id TEXT,
            timestamp_ms BIGINT NOT NULL,
            ip TEXT,
            user_agent TEXT,
            referrer TEXT,
            landing_page TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            utm_content TEXT,
            utm_term TEXT,
            utm_id TEXT,
            fbclid TEXT,
            fbc TEXT,
            fbp TEXT,
            ttclid TEXT,
            gclid TEXT,
            msclkid TEXT,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,

        // Tabela de vendas
        `CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            sale_code TEXT UNIQUE NOT NULL,
            click_id TEXT,
            customer_name TEXT,
            customer_email TEXT,
            customer_phone TEXT,
            customer_document TEXT,
            plan_name TEXT,
            plan_value DECIMAL(10,2),
            currency TEXT DEFAULT 'BRL',
            payment_platform TEXT,
            payment_method TEXT,
            status TEXT DEFAULT 'pending',
            ip TEXT,
            user_agent TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            utm_content TEXT,
            utm_term TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            approved_at TIMESTAMP,
            facebook_sent BOOLEAN DEFAULT FALSE,
            tiktok_sent BOOLEAN DEFAULT FALSE,
            utmify_sent BOOLEAN DEFAULT FALSE
        )`,

        // Tabela de pixels
        `CREATE TABLE IF NOT EXISTS pixels (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            platform TEXT NOT NULL,
            pixel_id TEXT NOT NULL,
            event_source_id TEXT,
            access_token TEXT NOT NULL,
            test_event_code TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(platform, pixel_id)
        )`
    ];

    // Depois, criar os índices separadamente
    const createIndexes = [
        `CREATE INDEX IF NOT EXISTS idx_clicks_click_id ON clicks(click_id)`,
        `CREATE INDEX IF NOT EXISTS idx_clicks_ttclid ON clicks(ttclid)`,
        `CREATE INDEX IF NOT EXISTS idx_sales_sale_code ON sales(sale_code)`,
        `CREATE INDEX IF NOT EXISTS idx_sales_click_id ON sales(click_id)`
    ];

    const client = await pool.connect();
    try {
        // Criar tabelas
        for (const query of createTables) {
            await client.query(query);
        }
        console.log('✅ Tabelas criadas!');

        // Criar índices
        for (const query of createIndexes) {
            await client.query(query);
        }
        console.log('✅ Índices criados!');

    } catch (error) {
        console.error('❌ Erro ao configurar banco:', error.message);
        console.error('Query que falhou:', error.query || 'N/A');
        throw error;
    } finally {
        client.release();
    }
}

function brTimestampToUTC(timestampSeconds) {
    const ms = timestampSeconds * 1000;
    const utcDate = new Date(ms + 3 * 60 * 60 * 1000); // BR → UTC
    return utcDate;
}

// Formato exigido pela UTMify
function toUTMDate(d) {
    return new Date(d).toISOString().replace("T", " ").substring(0, 19);
}

// --- FUNÇÕES AUXILIARES ---
function hashData(data) {
    if (!data || typeof data !== 'string') return null;
    return crypto.createHash('sha256')
        .update(data.toLowerCase().trim())
        .digest('hex');
}

function normalizePlanValue(value, source = 'apex') {
    if (!value && value !== 0) {
        console.log(`🔍 normalizePlanValue: valor nulo ou undefined`);
        return 0;
    }

    console.log(`🔍 DEBUG VALOR INICIAL:`, {
        valor: value,
        tipo: typeof value,
        fonte: source
    });

    let numValue;
    if (typeof value === 'string') {
        // Remove R$, pontos como separador de milhar, espaços
        const cleaned = value
            .replace(/[R$\s]/g, '')  // Remove R$ e espaços
            .replace(/\./g, '')       // Remove pontos (separadores de milhar)
            .replace(',', '.');       // Converte vírgula decimal para ponto

        numValue = parseFloat(cleaned);
        console.log(`🔍 STRING LIMPA: "${value}" → "${cleaned}" → ${numValue}`);
    } else {
        numValue = parseFloat(value);
        console.log(`🔍 NUMÉRICO DIRETO: ${value} → ${numValue}`);
    }

    if (isNaN(numValue)) {
        console.log(`❌ VALOR INVÁLIDO: ${value} retornou NaN`);
        return 0;
    }

    // Apex Vips envia em centavos (ex: 4990 = R$49,90)
    if (source === 'apex') {
        // Se o valor for maior que 100 e não tiver casas decimais, provavelmente está em centavos
        if (numValue > 100 && numValue === Math.floor(numValue)) {
            const emReais = numValue / 100;
            console.log(`💰 CONVERTIDO CENTAVOS→REAIS: ${numValue} → ${emReais}`);
            return emReais;
        }
    }

    console.log(`✅ VALOR FINAL: ${numValue}`);
    return numValue;
}

// --- FUNÇÕES DO BANCO ---

// 1. Salvar clique
async function saveClick(data) {
    const query = `
        INSERT INTO clicks (
            click_id, session_id, timestamp_ms, ip, user_agent, referrer, landing_page,
            utm_source, utm_medium, utm_campaign, utm_content, utm_term, utm_id,
            fbclid, fbc, fbp, ttclid, gclid, msclkid
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        ON CONFLICT (click_id) DO NOTHING
        RETURNING id;
    `;

    const values = [
        data.click_id,
        data.session_id || `session_${Date.now()}`,
        data.timestamp_ms || Date.now(),
        data.ip,
        data.user_agent,
        data.referrer || '',
        data.landing_page || '',
        data.utm_source,
        data.utm_medium,
        data.utm_campaign,
        data.utm_content,
        data.utm_term,
        data.utm_id,
        data.fbclid,
        data.fbc,
        data.fbp,
        data.ttclid,
        data.gclid,
        data.msclkid
    ];

    try {
        const result = await pool.query(query, values);
        return { success: true, id: result.rows[0]?.id };
    } catch (error) {
        console.error('❌ Erro ao salvar clique:', error.message);
        return { success: false, error: error.message };
    }
}

// 2. Buscar clique - ATUALIZADA
async function getClick(clickId) {
    if (!clickId) return null;

    console.log(`🔍 Buscando click: "${clickId}"`);

    // Primeiro tentar busca exata
    const exactQuery = 'SELECT * FROM clicks WHERE click_id = $1 LIMIT 1';
    try {
        const exactResult = await pool.query(exactQuery, [clickId]);

        if (exactResult.rows.length > 0) {
            console.log(`✅ Click encontrado (busca exata): ${clickId}`);
            return exactResult.rows[0];
        }

        // Se não encontrou, tentar busca avançada
        console.log(`⚠️ Click não encontrado (busca exata), tentando busca avançada...`);
        return await findClickByMultipleCriteria(clickId);

    } catch (error) {
        console.error('❌ Erro ao buscar clique:', error.message);
        return null;
    }
}

// 3. Salvar venda (ATUALIZADA)
async function saveSale(data) {
    // CORREÇÃO: Garantir que plan_value seja número
    const planValue = typeof data.plan_value === 'string'
        ? parseFloat(data.plan_value.replace(',', '.'))
        : parseFloat(data.plan_value || 0);

    // CORREÇÃO: Se vem da Apex Vips (em centavos), converter
    const finalPlanValue = planValue > 10000 ? planValue / 100 : planValue;

    const query = `
        INSERT INTO sales (
            sale_code, click_id, customer_name, customer_email, customer_phone,
            customer_document, plan_name, plan_value, currency, payment_platform,
            payment_method, status, ip, user_agent, utm_source, utm_medium,
            utm_campaign, utm_content, utm_term, approved_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
        ON CONFLICT (sale_code) DO UPDATE SET
            status = EXCLUDED.status,
            approved_at = EXCLUDED.approved_at,
            customer_email = COALESCE(EXCLUDED.customer_email, sales.customer_email),
            customer_phone = COALESCE(EXCLUDED.customer_phone, sales.customer_phone),
            plan_value = COALESCE(EXCLUDED.plan_value, sales.plan_value),
            utm_source = COALESCE(EXCLUDED.utm_source, sales.utm_source),
            utm_medium = COALESCE(EXCLUDED.utm_medium, sales.utm_medium),
            utm_campaign = COALESCE(EXCLUDED.utm_campaign, sales.utm_campaign),
            utm_content = COALESCE(EXCLUDED.utm_content, sales.utm_content),
            utm_term = COALESCE(EXCLUDED.utm_term, sales.utm_term),
            click_id = COALESCE(EXCLUDED.click_id, sales.click_id)
        RETURNING id;
    `;

    const values = [
        data.sale_code || `sale_${Date.now()}`,
        data.click_id,
        data.customer_name || 'Cliente Apex',
        data.customer_email || 'naoinformado@apexvips.com',
        data.customer_phone,
        data.customer_document,
        data.plan_name || 'Plano Apex',
        finalPlanValue, // CORREÇÃO: Valor convertido
        data.currency || 'BRL',
        data.payment_platform || 'apexvips',
        data.payment_method || 'unknown',
        data.status || 'pending',
        data.ip || '0.0.0.0',
        data.user_agent || 'ApexVips/1.0',
        data.utm_source,
        data.utm_medium,
        data.utm_campaign,
        data.utm_content || '',
        data.utm_term || '',
        data.approved_at ? new Date(data.approved_at * 1000) : null
    ];

    try {
        const result = await pool.query(query, values);
        return { success: true, id: result.rows[0]?.id };
    } catch (error) {
        console.error('❌ Erro ao salvar venda:', error.message);
        return { success: false, error: error.message };
    }
}

// 4. Buscar pixels ativos
async function getActivePixels(platform = null) {
    let query = 'SELECT * FROM pixels WHERE is_active = TRUE';
    const values = [];

    if (platform) {
        query += ' AND platform = $1';
        values.push(platform);
    }

    try {
        const result = await pool.query(query, values);
        return result.rows;
    } catch (error) {
        console.error('❌ Erro ao buscar pixels:', error.message);
        return [];
    }
}

// 3.1 Buscar venda por sale_code (NOVA)
async function getSaleBySaleCode(saleCode) {
    if (!saleCode) return null;

    try {
        const result = await pool.query(
            'SELECT * FROM sales WHERE sale_code = $1 LIMIT 1',
            [saleCode]
        );

        if (result.rows.length > 0) {
            return result.rows[0];
        }

        return null;
    } catch (error) {
        console.error('❌ Erro ao buscar venda por sale_code:', error.message);
        return null;
    }
}

async function sendToUtmify(saleData, clickData) {
    console.log(`📤 ENVIANDO PARA UTMIFY: ${saleData.sale_code}`);
    console.log(`💰 VALOR RECEBIDO: ${saleData.plan_value} (tipo: ${typeof saleData.plan_value})`);

    if (!UTMIFY_API_KEY) {
        console.log('⚠️ UTMIFY_API_KEY não configurada');
        return { success: false, error: 'API key não configurada' };
    }

    try {
        const isTest = saleData.sale_code.includes('TEST');

        // Mapear status
        let utmifyStatus;
        if (saleData.status === 'approved' || saleData.status === 'paid') {
            utmifyStatus = 'paid';
        } else if (saleData.status === 'created' || saleData.status === 'pending') {
            utmifyStatus = 'waiting_payment';
        } else {
            utmifyStatus = saleData.status;
        }

        // DEBUG DETALHADO DO VALOR
        console.log('🔍 DEBUG DETALHADO DO VALOR UTMIFY:');
        console.log('- Valor bruto:', saleData.plan_value);
        console.log('- Tipo:', typeof saleData.plan_value);

        // CORREÇÃO CRÍTICA: Converter para número corretamente
        let valorNumerico;
        if (typeof saleData.plan_value === 'string') {
            // Remove caracteres não numéricos exceto ponto, vírgula e hífen
            const limpo = saleData.plan_value
                .replace(/[^\d,.-]/g, '')  // Remove tudo exceto números, vírgula, ponto e hífen
                .replace(/\./g, '')        // Remove pontos (separadores de milhar)
                .replace(',', '.');        // Converte vírgula decimal para ponto

            valorNumerico = parseFloat(limpo);
            console.log('- String limpa:', limpo);
        } else {
            valorNumerico = parseFloat(saleData.plan_value);
        }

        if (isNaN(valorNumerico)) {
            console.error('❌ ERRO: Não foi possível converter valor para número:', saleData.plan_value);
            valorNumerico = 0;
        }

        console.log('- Valor numérico:', valorNumerico);

        // CORREÇÃO: UTMify espera valor em centavos
        // Primeiro garante que estamos lidando com reais (não centavos)
        let valorEmReais = valorNumerico;

        // Se o valor for muito grande (ex: 189000) e parece ser em centavos, converte para reais
        if (valorEmReais > 1000 && valorEmReais === Math.floor(valorEmReais)) {
            console.log('⚠️ Valor parece estar em centavos, convertendo para reais...');
            valorEmReais = valorEmReais / 100;
            console.log(`- Convertido: ${valorNumerico} → ${valorEmReais}`);
        }

        const priceInCents = Math.round(valorEmReais * 100);

        console.log('- Valor em reais:', valorEmReais.toFixed(2));
        console.log('- Em centavos (para UTMify):', priceInCents);
        console.log('- Equivalente: R$', (priceInCents / 100).toFixed(2));

        // CORREÇÃO: Função melhorada para datas
        const formatUTCDate = (dateInput) => {
            if (!dateInput) return null;

            let date;

            // Se for timestamp em segundos (do webhook)
            if (typeof dateInput === 'number' && dateInput < 10000000000) {
                date = new Date(dateInput * 1000);
            }
            // Se for string de data
            else if (typeof dateInput === 'string') {
                date = new Date(dateInput);
            }
            // Se for objeto Date
            else if (dateInput instanceof Date) {
                date = dateInput;
            }
            // Se for número em ms
            else if (typeof dateInput === 'number') {
                date = new Date(dateInput);
            }
            else {
                return null;
            }

            if (isNaN(date.getTime())) return null;

            const pad = (n) => n.toString().padStart(2, '0');
            const year = date.getUTCFullYear();
            const month = pad(date.getUTCMonth() + 1);
            const day = pad(date.getUTCDate());
            const hours = pad(date.getUTCHours());
            const minutes = pad(date.getUTCMinutes());
            const seconds = pad(date.getUTCSeconds());

            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
        };

        // CORREÇÃO: Lógica de datas prioritária
        let createdAt;
        if (saleData.created_at) {
            createdAt = formatUTCDate(saleData.created_at);
        } else if (saleData.timestamp) {
            createdAt = formatUTCDate(saleData.timestamp);
        } else {
            // Buscar do banco se existir
            const existingSale = await pool.query(
                'SELECT created_at FROM sales WHERE sale_code = $1',
                [saleData.sale_code]
            );
            if (existingSale.rows[0]?.created_at) {
                createdAt = formatUTCDate(existingSale.rows[0].created_at);
            } else {
                createdAt = formatUTCDate(Date.now());
            }
        }

        // Approved date apenas se paid
        let approvedDate = null;
        if (utmifyStatus === 'paid') {
            if (saleData.approved_at) {
                approvedDate = formatUTCDate(saleData.approved_at);
            } else if (saleData.timestamp) {
                approvedDate = formatUTCDate(saleData.timestamp);
            } else {
                approvedDate = createdAt; // Usa mesma data de criação
            }
        }

        const payload = {
            orderId: saleData.sale_code,
            platform: saleData.payment_platform || "ApexVips",
            paymentMethod: saleData.payment_method || "pix",
            status: utmifyStatus,

            createdAt: createdAt,
            approvedDate: approvedDate,
            refundedAt: null,

            customer: {
                name: saleData.customer_name || "Cliente",
                email: saleData.customer_email || "nao@apexvips.com",
                phone: saleData.customer_phone || null,
                document: saleData.customer_document || null,
                country: "BR",
                ip: saleData.ip || clickData?.ip || "0.0.0.0"
            },

            products: [
                {
                    id: saleData.plan_name?.toLowerCase().replace(/[^a-z0-9]/g, '-') || "product",
                    name: saleData.plan_name || "Produto",
                    planId: null,
                    planName: saleData.plan_name || null,
                    quantity: 1,
                    priceInCents: priceInCents
                }
            ],

            trackingParameters: {
                src: null,
                sck: null,
                utm_source: clickData?.utm_source || saleData.utm_source || null,
                utm_campaign: clickData?.utm_campaign || saleData.utm_campaign || null,
                utm_medium: clickData?.utm_medium || saleData.utm_medium || null,
                utm_content: clickData?.utm_content || saleData.utm_content || null,
                utm_term: clickData?.utm_term || saleData.utm_term || null
            },

            commission: {
                totalPriceInCents: priceInCents,
                gatewayFeeInCents: 0,
                userCommissionInCents: priceInCents,
                currency: "BRL"
            }
        };

        if (isTest) {
            payload.isTest = true;
        }

        console.log('📦 Payload UTMify:', JSON.stringify(payload, null, 2));

        const response = await axios.post(
            "https://api.utmify.com.br/api-credentials/orders",
            payload,
            {
                headers: {
                    "x-api-token": UTMIFY_API_KEY,
                    "Content-Type": "application/json"
                },
                timeout: 15000
            }
        );

        await pool.query(
            "UPDATE sales SET utmify_sent = TRUE WHERE sale_code = $1",
            [saleData.sale_code]
        );

        console.log(`✅ UTMify: Evento enviado com sucesso! Valor: R$ ${(priceInCents / 100).toFixed(2)}`);

        return { success: true, data: response.data };

    } catch (err) {
        console.error("❌ Erro UTMify:", err.response?.data || err.message);
        console.error("📦 Payload que falhou:", JSON.stringify(payload, null, 2));
        return { success: false, error: err.message };
    }
}

// --- FUNÇÕES DE PIXEL ---

async function sendTikTokEvent(pixel, eventData, clickData, isTest = false) {
    console.log(`🎯 ENVIANDO TIKTOK: ${eventData.sale_code}`);

    const url = 'https://business-api.tiktok.com/open_api/v1.3/event/track/';

    // CORREÇÃO: TikTok web não usa event_source_id ou usa domain
    const eventSourceId = ''; // Deixar vazio para web
    // OU usar domain se disponível
    // const landingDomain = clickData?.landing_page ? new URL(clickData.landing_page).hostname : '';

    // Hash dos dados do usuário
    const user = {};
    if (eventData.customer_email) {
        user.email = hashData(eventData.customer_email.toLowerCase().trim());
    }
    if (eventData.customer_phone) {
        const phoneClean = eventData.customer_phone.replace(/\D/g, '');
        user.phone = hashData(phoneClean);
    }
    if (eventData.customer_document) {
        const docClean = eventData.customer_document.replace(/\D/g, '');
        user.external_id = hashData(docClean);
    }

    if (clickData?.ip) user.ip = clickData.ip;
    if (eventData.user_agent) user.user_agent = eventData.user_agent;

    // CORREÇÃO: Valor com 2 casas decimais
    const value = parseFloat(eventData.plan_value || 0);
    const valueFixed = parseFloat(value.toFixed(2));

    const payload = {
        event_source: "web",
        event_source_id: eventSourceId, // CORREÇÃO: vazio para web
        data: [
            {
                event: "Purchase",
                event_time: Math.floor(Date.now() / 1000),
                event_id: eventData.sale_code,
                user: user,
                properties: {
                    currency: eventData.currency || "BRL",
                    value: valueFixed, // CORREÇÃO: 2 casas decimais
                    contents: [
                        {
                            content_id: "vip_access",
                            content_name: eventData.plan_name || "Acesso VIP",
                            quantity: 1,
                            price: valueFixed // CORREÇÃO: 2 casas decimais
                        }
                    ],
                    content_type: "product"
                },
                page: {
                    url: clickData?.landing_page || "https://apexvips.com",
                    referrer: clickData?.referrer || ""
                }
            }
        ]
    };

    if (clickData?.ttclid) {
        payload.data[0].context = {
            ad: {
                callback: clickData.ttclid
            }
        };
    }

    if (isTest) {
        payload.test_event_code = pixel.test_event_code || "TEST54815";
    }

    try {
        const response = await axios.post(url, payload, {
            headers: {
                "Access-Token": pixel.access_token,
                "Content-Type": "application/json"
            },
            timeout: 10000
        });

        return { success: true, data: response.data };

    } catch (err) {
        console.error(`❌ Erro TikTok:`, err.response?.data || err.message);
        return { success: false, error: err.message };
    }
}

// Enviar evento para Facebook - CORRIGIDA
// CORRIGIR a função sendFacebookEvent
async function sendFacebookEvent(pixel, eventData, clickData, isTest = false) {
    const url = `https://graph.facebook.com/v19.0/${pixel.pixel_id}/events`;

    // Preparar dados do usuário
    const userData = {
        client_ip_address: eventData.ip || clickData?.ip || '',
        client_user_agent: eventData.user_agent || clickData?.user_agent || '',
        fbc: clickData?.fbc,
        fbp: clickData?.fbp
    };

    if (eventData.customer_email) {
        userData.em = [hashData(eventData.customer_email)];
    }
    if (eventData.customer_phone) {
        userData.ph = [hashData(eventData.customer_phone.replace(/\D/g, ''))];
    }
    if (eventData.customer_document) {
        userData.external_id = [hashData(eventData.customer_document.replace(/\D/g, ''))];
    }

    // Remover campos undefined/vazios
    Object.keys(userData).forEach(key => {
        if (!userData[key] || (Array.isArray(userData[key]) && userData[key].length === 0)) {
            delete userData[key];
        }
    });

    // 🔥🔥🔥 CORREÇÃO CRÍTICA: Facebook NÃO usa centavos!
    // Valor deve ser em reais com 2 casas decimais
    const value = parseFloat(eventData.plan_value || 0);
    const valueInReais = parseFloat(value.toFixed(2)); // Formata para 2 casas decimais

    const payload = {
        data: [{
            event_name: 'Purchase',
            event_time: Math.floor(Date.now() / 1000),
            event_id: eventData.sale_code,
            action_source: 'website',
            user_data: userData,
            custom_data: {
                value: valueInReais, // 🔥 CORREÇÃO: Valor em REAIS, não centavos
                currency: eventData.currency || 'BRL'
            }
        }],
        access_token: pixel.access_token
    };

    // CORREÇÃO: Só enviar test_event_code se for teste
    if (isTest) {
        payload.test_event_code = pixel.test_event_code || 'TEST54815';
    }

    // Log para debug
    console.log(`📘 Facebook Event: ${eventData.sale_code}`);
    console.log(`💰 Valor: R$ ${valueInReais.toFixed(2)} (NÃO em centavos!)`);
    console.log(`👤 Dados usuário:`, Object.keys(userData).length > 0 ? 'Sim' : 'Não');

    try {
        const response = await axios.post(url, payload);
        console.log(`✅ Facebook: Evento enviado para ${eventData.sale_code} - Valor: R$ ${valueInReais}`);
        return { success: true, data: response.data };
    } catch (error) {
        console.error('❌ Facebook Error:', error.response?.data || error.message);
        console.error('📦 Payload enviado:', JSON.stringify(payload, null, 2));
        return { success: false, error: error.message };
    }
}

// Processar eventos de pixel após venda
async function processPixelEvents(saleData, clickData, isTest = false) {
    const pixels = await getActivePixels();

    const results = [];
    for (const pixel of pixels) {
        try {
            let result;

            if (pixel.platform === 'tiktok') {
                result = await sendTikTokEvent(pixel, saleData, clickData, isTest);
            } else if (pixel.platform === 'facebook') {
                result = await sendFacebookEvent(pixel, saleData, clickData, isTest);
            }

            // Atualizar status na venda (apenas se não for teste)
            if (result?.success && !isTest) {
                const column = pixel.platform === 'tiktok' ? 'tiktok_sent' : 'facebook_sent';
                await pool.query(
                    `UPDATE sales SET ${column} = TRUE WHERE sale_code = $1`,
                    [saleData.sale_code]
                );
            }

            results.push({
                platform: pixel.platform,
                success: result?.success || false,
                error: result?.error,
                test_mode: isTest
            });

        } catch (error) {
            console.error(`❌ Erro ao processar pixel ${pixel.platform}:`, error.message);
            results.push({ platform: pixel.platform, success: false, error: error.message, test_mode: isTest });
        }
    }

    return results;
}

// --- ROTAS ---

// Rota para diagnóstico completo
app.get('/api/diagnose/:sale_code', async (req, res) => {
    try {
        const saleCode = req.params.sale_code;

        console.log(`🔍 DIAGNÓSTICO para: ${saleCode}`);

        // 1. Buscar venda
        const saleResult = await pool.query(
            'SELECT * FROM sales WHERE sale_code = $1 LIMIT 1',
            [saleCode]
        );

        if (saleResult.rows.length === 0) {
            return res.json({ error: 'Venda não encontrada' });
        }

        const sale = saleResult.rows[0];

        // 2. Buscar click associado
        const clickResult = await pool.query(
            'SELECT * FROM clicks WHERE click_id = $1 LIMIT 1',
            [sale.click_id || sale.sale_code]
        );

        const click = clickResult.rows[0] || null;

        // 3. Buscar pixels ativos
        const pixels = await getActivePixels();

        // 4. Simular envio UTMify
        const utmifyResult = await sendToUtmify(sale, click);

        // 5. Simular envio TikTok
        const tiktokPixel = pixels.find(p => p.platform === 'tiktok');
        let tiktokResult = null;
        if (tiktokPixel) {
            tiktokResult = await sendTikTokEvent(tiktokPixel, sale, click, true);
        }

        res.json({
            success: true,
            sale: {
                ...sale,
                has_utm: !!(sale.utm_source || sale.utm_campaign),
                has_click_id: !!sale.click_id
            },
            click: click ? {
                click_id: click.click_id,
                ttclid: click.ttclid || '❌ NÃO ENCONTRADO',
                fbclid: click.fbclid || '❌ NÃO ENCONTRADO',
                utm_source: click.utm_source,
                utm_campaign: click.utm_campaign,
                landing_page: click.landing_page,
                received_at: click.received_at
            } : null,
            pixels: pixels.map(p => ({
                platform: p.platform,
                name: p.name,
                pixel_id: p.pixel_id,
                has_access_token: !!p.access_token
            })),
            tests: {
                utmify: utmifyResult,
                tiktok: tiktokResult
            },
            recommendations: [
                !click ? "❌ Nenhum click associado - o ttclid não será enviado" : "",
                click && !click.ttclid ? "⚠️ Click não tem ttclid - TikTok não conseguirá atribuir" : "",
                !tiktokPixel ? "❌ Nenhum pixel TikTok configurado" : ""
            ].filter(r => r)
        });

    } catch (error) {
        console.error('❌ Erro no diagnóstico:', error);
        res.status(500).json({ error: error.message });
    }
});

// Rota 0: Validação GET para Apex Vips
app.get('/api/webhook/apex', (req, res) => {
    console.log('✅ Validação GET do webhook recebida');

    res.json({
        status: 'active',
        message: 'Webhook configurado e funcionando',
        webhook_url: 'https://utm-ujn8.onrender.com/api/webhook/apex',
        supported_events: ['user_joined', 'payment_created', 'payment_approved'],
        example_payload: {
            event: "payment_approved",
            timestamp: 1732252000,
            bot_id: 123456789,
            customer: {
                chat_id: 987654321,
                profile_name: "John Doe",
                phone: "+5511999999999"
            },
            transaction: {
                sale_code: "SALE-XYZ789",
                plan_value: 4990,
                currency: "BRL"
            }
        },
        server_time: new Date().toISOString(),
        version: "1.0.0"
    });
});

// Rota de debug para ver clicks recentes
app.get('/admin/debug/clicks', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                click_id,
                utm_source,
                utm_campaign,
                utm_content,
                ttclid,
                received_at
            FROM clicks 
            ORDER BY received_at DESC 
            LIMIT 20
        `);

        res.json({
            success: true,
            count: result.rows.length,
            clicks: result.rows
        });

    } catch (error) {
        console.error('❌ Erro ao buscar clicks:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Rota para verificar click específico
app.get('/verify-click/:click_id', async (req, res) => {
    try {
        const clickId = req.params.click_id;

        const result = await pool.query(
            `SELECT 
                click_id,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_content,
                utm_term,
                utm_id,
                ttclid,
                fbclid,
                gclid,
                landing_page,
                referrer,
                received_at
            FROM clicks 
            WHERE click_id = $1`,
            [clickId]
        );

        if (result.rows.length === 0) {
            return res.json({
                success: false,
                message: 'Click não encontrado no banco de dados',
                click_id: clickId
            });
        }

        const clickData = result.rows[0];

        res.json({
            success: true,
            message: 'Click encontrado!',
            click_id: clickId,
            data: clickData,
            has_utm: !!clickData.utm_source,
            timestamp: clickData.received_at,
            utm_params: {
                source: clickData.utm_source,
                medium: clickData.utm_medium,
                campaign: clickData.utm_campaign,
                content: clickData.utm_content,
                term: clickData.utm_term,
                id: clickData.utm_id
            },
            platform_ids: {
                ttclid: clickData.ttclid,
                fbclid: clickData.fbclid,
                gclid: clickData.gclid
            }
        });

    } catch (error) {
        console.error('❌ Erro ao verificar click:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Últimos 10 cliques
app.get('/recent-clicks', async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT 
                click_id,
                utm_source,
                utm_campaign,
                utm_content,
                ttclid,
                landing_page,
                received_at
            FROM clicks 
            ORDER BY received_at DESC 
            LIMIT 10
        `);

        res.json({
            success: true,
            count: result.rows.length,
            clicks: result.rows.map(row => ({
                click_id: row.click_id,
                utm_source: row.utm_source || '(vazio)',
                utm_campaign: row.utm_campaign || '(vazio)',
                utm_content: row.utm_content || '(vazio)',
                ttclid: row.ttclid ? 'SIM' : 'NÃO',
                received_at: row.received_at,
                has_utm: !!(row.utm_source || row.utm_campaign)
            }))
        });

    } catch (error) {
        console.error('❌ Erro ao buscar clicks recentes:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Rota para buscar parâmetros UTM salvos
app.get('/api/utm-params/:click_id', async (req, res) => {
    try {
        const clickId = req.params.click_id;

        // Buscar do banco de dados
        const result = await pool.query(
            'SELECT utm_source, utm_medium, utm_campaign, utm_content, utm_term, utm_id, ttclid FROM clicks WHERE click_id = $1',
            [clickId]
        );

        if (result.rows.length > 0) {
            res.json({
                success: true,
                click_id: clickId,
                utm_params: {
                    utm_source: result.rows[0].utm_source,
                    utm_medium: result.rows[0].utm_medium,
                    utm_campaign: result.rows[0].utm_campaign,
                    utm_content: result.rows[0].utm_content,
                    utm_term: result.rows[0].utm_term,
                    utm_id: result.rows[0].utm_id,
                    ttclid: result.rows[0].ttclid
                }
            });
        } else {
            res.json({
                success: false,
                message: 'Click não encontrado',
                click_id: clickId
            });
        }

    } catch (error) {
        console.error('❌ Erro ao buscar UTM params:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Rota 1: Health Check
app.get('/', (req, res) => {
    res.json({
        status: 'online',
        service: 'tracking-api',
        version: '1.0.0',
        timestamp: new Date().toISOString(),
        features: ['tracking', 'webhooks', 'pixels', 'utmify']
    });
});

// ===============================
// FIXAR TODAS AS DATAS DO BANCO (OPÇÃO A - DATAS = HOJE) + REPROCESSAR
// ===============================

app.post("/admin/fix-dates-and-reprocess", async (req, res) => {
    try {
        console.log("\n🔧 Iniciando correção automática de datas (OPÇÃO A - DATAS HOJE)...\n");

        // Buscar todas as vendas
        const result = await pool.query("SELECT * FROM sales ORDER BY created_at ASC");
        const sales = result.rows;

        const summary = {
            total: sales.length,
            fixed: 0,
            reprocessed: 0,
            failed: 0,
            details: []
        };

        // 🕒 DATA ATUAL EM UTC (para createdAt e approvedAt)
        const nowUTC = new Date();
        const nowUTCFormatted = nowUTC.toISOString().replace("T", " ").substring(0, 19);

        for (const sale of sales) {
            try {

                // 🎯 Todas as vendas agora terão createdAt = HOJE
                const finalCreatedUTC = nowUTCFormatted;
                const finalApprovedUTC = sale.status === "approved" ? nowUTCFormatted : null;

                // 1️⃣ Atualizar no banco
                await pool.query(
                    `UPDATE sales 
                     SET created_at = $1, approved_at = $2
                     WHERE sale_code = $3`,
                    [finalCreatedUTC, finalApprovedUTC, sale.sale_code]
                );

                summary.fixed++;

                // 2️⃣ Recuperar UTMs reais
                const clickData = await recoverUTM(sale);

                // 3️⃣ Montar saleData corrigido
                const saleData = {
                    sale_code: sale.sale_code,
                    click_id: sale.click_id || sale.sale_code,

                    customer_name: sale.customer_name,
                    customer_email: sale.customer_email,
                    customer_phone: sale.customer_phone,
                    customer_document: sale.customer_document,

                    plan_name: sale.plan_name,
                    plan_value: sale.plan_value,
                    currency: sale.currency,
                    payment_platform: sale.payment_platform,
                    payment_method: sale.payment_method,

                    ip: sale.ip,
                    user_agent: sale.user_agent,

                    utm_source: clickData?.utm_source || sale.utm_source,
                    utm_medium: clickData?.utm_medium || sale.utm_medium,
                    utm_campaign: clickData?.utm_campaign || sale.utm_campaign,
                    utm_content: clickData?.utm_content || sale.utm_content,
                    utm_term: clickData?.utm_term || sale.utm_term,

                    status: sale.status,
                    created_at: finalCreatedUTC,
                    approved_at: finalApprovedUTC
                };

                // 4️⃣ Enviar para UTMify
                const utmRes = await sendToUtmify(saleData, clickData);

                // 5️⃣ Enviar para TikTok / Facebook
                await processPixelEvents(saleData, clickData, false);

                summary.reprocessed++;

                summary.details.push({
                    sale_code: sale.sale_code,
                    fixed_date: true,
                    utmify: utmRes.success
                });

            } catch (internalErr) {
                console.error("❌ Erro interno ao corrigir venda:", sale.sale_code, internalErr);
                summary.failed++;
                summary.details.push({
                    sale_code: sale.sale_code,
                    error: internalErr.message
                });
            }
        }

        res.json({
            success: true,
            message: "Correção completa (TODAS AS DATAS = HOJE) + reprocessamento executado.",
            report: summary
        });

    } catch (err) {
        console.error("❌ ERRO CRÍTICO:", err);
        res.status(500).json({ success: false, error: err.message });
    }
});


// Rota 2: Receber cliques do frontend
app.post('/api/track', async (req, res) => {
    try {
        const data = req.body;

        if (!data.click_id) {
            return res.status(400).json({ error: 'click_id é obrigatório' });
        }

        // Adicionar IP
        data.ip = req.ip || req.headers['x-forwarded-for'] || req.connection.remoteAddress;

        // Salvar no banco
        const result = await saveClick(data);

        res.json({
            success: true,
            click_id: data.click_id,
            saved: result.success
        });

    } catch (error) {
        console.error('❌ Erro em /api/track:', error.message);
        res.status(500).json({ error: 'Erro interno' });
    }
});

// Rota 3: Pixel GIF para tracking simples
app.get('/pixel.gif', async (req, res) => {
    try {
        // Coletar dados da query
        const clickData = {
            click_id: req.query.click_id || `pixel_${Date.now()}`,
            timestamp_ms: Date.now(),
            ip: req.ip || req.headers['x-forwarded-for'] || req.connection.remoteAddress,
            user_agent: req.headers['user-agent'],
            referrer: req.headers['referer'] || req.headers['referrer'],
            utm_source: req.query.utm_source || req.query.us,
            utm_medium: req.query.utm_medium || req.query.um,
            utm_campaign: req.query.utm_campaign || req.query.uc,
            fbclid: req.query.fbclid,
            ttclid: req.query.ttclid,
            gclid: req.query.gclid
        };

        // Salvar assincronamente
        saveClick(clickData).catch(console.error);

        // Retornar GIF 1x1 transparente
        const pixel = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', 'base64');
        res.writeHead(200, {
            'Content-Type': 'image/gif',
            'Content-Length': pixel.length,
            'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0'
        });
        res.end(pixel);

    } catch (error) {
        // Sempre retornar o pixel
        const pixel = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', 'base64');
        res.writeHead(200, { 'Content-Type': 'image/gif' });
        res.end(pixel);
    }
});

// Rota 4: Webhook da Apex Vips (ATUALIZADA)
app.post('/api/webhook/apex', async (req, res) => {
    console.log('📨 Webhook recebido da Apex Vips');
    console.log('Body recebido:', JSON.stringify(req.body, null, 2));

    try {
        const eventData = req.body;

        // Validar formato da Apex Vips
        if (!eventData.event || !eventData.bot_id) {
            console.log('⚠️ Dados inválidos: evento ou bot_id faltando');
            return res.status(400).json({
                success: false,
                error: 'Formato inválido. Evento ou bot_id faltando.'
            });
        }

        // Responder imediatamente para evitar timeout
        res.json({
            success: true,
            message: 'Webhook recebido com sucesso',
            event: eventData.event,
            sale_code: eventData.transaction?.sale_code || 'N/A',
            received_at: new Date().toISOString()
        });

        // Processar em segundo plano (assíncrono)
        setTimeout(async () => {
            try {
                // Processar apenas eventos de pagamento
                if (eventData.event === 'payment_approved' || eventData.event === 'payment_created') {
                    await processApexEvent(eventData);
                } else if (eventData.event === 'user_joined') {
                    console.log('👤 Usuário entrou:', eventData.customer?.profile_name);
                } else {
                    console.log(`📝 Evento recebido: ${eventData.event}`);
                }
            } catch (error) {
                console.error('❌ Erro no processamento assíncrono:', error.message);
            }
        }, 100);

    } catch (error) {
        console.error('❌ Erro no webhook:', error.message);
        // Sempre retornar 200 para a Apex
        res.status(200).json({
            success: false,
            error: 'Erro interno',
            timestamp: new Date().toISOString()
        });
    }
});

// Função para verificar UTM parameters no banco
async function checkUtmForSale(saleCode, clickId) {
    try {
        let query = `SELECT 
                sale_code,
                click_id,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_content,
                utm_term
            FROM sales 
            WHERE sale_code = $1`;

        const result = await pool.query(query, [saleCode]);

        if (result.rows.length > 0) {
            const data = result.rows[0];

            // Se tiver click_id, buscar dados do click também
            if (data.click_id) {
                const clickResult = await pool.query(
                    `SELECT 
                        click_id,
                        utm_source as click_utm_source,
                        utm_medium as click_utm_medium,
                        utm_campaign as click_utm_campaign,
                        utm_content as click_utm_content,
                        utm_term as click_utm_term,
                        landing_page,
                        referrer,
                        ttclid,
                        fbclid,
                        gclid
                    FROM clicks 
                    WHERE click_id = $1`,
                    [data.click_id]
                );

                if (clickResult.rows.length > 0) {
                    const clickData = clickResult.rows[0];
                    return {
                        sale_data: data,
                        click_data: clickData
                    };
                }
            }

            return {
                sale_data: data,
                click_data: null
            };
        }
        return null;
    } catch (error) {
        console.error('❌ Erro ao verificar UTM:', error.message);
        return null;
    }
}

// Função para processar eventos da Apex (NOVA VERSÃO)
// =======================
// PROCESSAR EVENTO APEX VIPS (NOVO, ATUALIZADO E CORRIGIDO)
// =======================

async function processApexEvent(eventData) {
    console.log("\n" + "=".repeat(50));
    console.log("💰 PROCESSANDO EVENTO APEX:", eventData.event);
    console.log("📦 Dados recebidos:", JSON.stringify(eventData, null, 2));
    console.log("=".repeat(50) + "\n");

    try {
        console.log("💰 PROCESSANDO EVENTO APEX:", eventData.event);

        // 1️⃣ Capturar datas
        const timestampBR = eventData.timestamp;
        const createdAtUTC = brTimestampToUTC(timestampBR);
        const approvedAtUTC =
            eventData.event === "payment_approved"
                ? brTimestampToUTC(timestampBR)
                : null;

        // 2️⃣ Identificar sale_code
        const saleCode =
            eventData.transaction?.sale_code ||
            eventData.transaction?.external_transaction_id ||
            `APEX_${eventData.timestamp}`;

        console.log(`📝 Sale Code identificado: ${saleCode}`);

        // 3️⃣ Buscar venda anterior
        const existing = await pool.query(
            "SELECT * FROM sales WHERE sale_code = $1 LIMIT 1",
            [saleCode]
        );

        const isUpdate = existing.rows.length > 0;
        console.log(`🔄 É atualização? ${isUpdate ? 'Sim' : 'Não'}`);

        // 🔥 CORREÇÃO: Usar normalizePlanValue DENTRO da função
        console.log(`🔍 VALOR DO WEBHOOK APEX: ${eventData.transaction?.plan_value}`);
        console.log(`📊 Tipo do valor original: ${typeof eventData.transaction?.plan_value}`);

        const normalizedPlanValue = normalizePlanValue(
            eventData.transaction?.plan_value,
            'apex'
        ) || existing.rows[0]?.plan_value || 0;

        console.log(`💰 VALOR NORMALIZADO: ${normalizedPlanValue}`);
        console.log(`📈 Tipo do valor normalizado: ${typeof normalizedPlanValue}`);
        console.log(`💎 Valor em reais: R$ ${normalizedPlanValue.toFixed(2)}`);
        console.log(`🔢 Valor em centavos: ${Math.round(normalizedPlanValue * 100)}`);

        // 4️⃣ Criar objeto base da venda
        const baseSaleData = {
            sale_code: saleCode,
            click_id: saleCode, // seu sistema usa sale_code como click_id
            customer_name:
                eventData.customer?.full_name ||
                eventData.customer?.profile_name ||
                existing.rows[0]?.customer_name ||
                "Cliente",
            customer_email:
                eventData.customer?.email ||
                `user_${eventData.customer?.chat_id}@apexvips.com`,
            customer_phone: eventData.customer?.phone || null,
            customer_document: eventData.customer?.tax_id || null,
            plan_name: eventData.transaction?.plan_name || existing.rows[0]?.plan_name,
            plan_value: normalizedPlanValue, // 🔥 Usando o valor normalizado
            currency: eventData.transaction?.currency || "BRL",
            payment_platform: eventData.transaction?.payment_platform || "ApexVips",
            payment_method: eventData.transaction?.payment_method || "pix",
            ip: eventData.origin?.ip || existing.rows[0]?.ip || "0.0.0.0",
            user_agent: eventData.origin?.user_agent || "ApexVipsBot/1.0",
            status:
                eventData.event === "payment_created" ? "created" :
                    eventData.event === "payment_approved" ? "approved" :
                        "pending",
            created_at: isUpdate ? existing.rows[0].created_at : createdAtUTC,
            approved_at: isUpdate
                ? existing.rows[0]?.approved_at
                : approvedAtUTC,

            // UTMs preenchidas depois
            utm_source: null,
            utm_medium: null,
            utm_campaign: null,
            utm_content: null,
            utm_term: null
        };

        console.log(`📋 Dados base da venda criados:`);
        console.log(`- Cliente: ${baseSaleData.customer_name}`);
        console.log(`- Plano: ${baseSaleData.plan_name}`);
        console.log(`- Valor: R$ ${baseSaleData.plan_value}`);
        console.log(`- Status: ${baseSaleData.status}`);

        // 5️⃣ Recuperar UTM real
        const clickData = await recoverUTM(baseSaleData);
        console.log("\n🔍 RESULTADO recoverUTM:");
        console.log("Click encontrado?", !!clickData);
        if (clickData) {
            console.log("- Click ID:", clickData.click_id);
            console.log("- TTCLID:", clickData.ttclid || "❌ AUSENTE");
            console.log("- FB Click ID:", clickData.fbclid || "❌ AUSENTE");
            console.log("- UTMs:", {
                source: clickData.utm_source,
                campaign: clickData.utm_campaign,
                medium: clickData.utm_medium,
                content: clickData.utm_content,
                term: clickData.utm_term
            });
        } else {
            console.log("⚠️ Nenhum click associado encontrado");
        }

        // ====================================================
        // 🔥 FALLBACK AUTOMÁTICO PARA MAILING / INTERNO
        // ====================================================
        const hasRealUTM = clickData?.utm_source || baseSaleData.utm_source;

        if (!hasRealUTM) {
            console.log("⚠️ Nenhuma UTM encontrada, usando fallback 'mailing'");
            baseSaleData.utm_source = "direct";
            baseSaleData.utm_medium = "internal";
            baseSaleData.utm_campaign = "mailing";
            baseSaleData.utm_content = baseSaleData.plan_name || "";
            baseSaleData.utm_term = "";
        }

        // Se click tiver UTM válida → usa ela
        if (clickData?.utm_source) {
            console.log("✅ UTMs reais encontradas, usando dados do click");
            baseSaleData.utm_source = clickData.utm_source;
            baseSaleData.utm_medium = clickData.utm_medium;
            baseSaleData.utm_campaign = clickData.utm_campaign;
            baseSaleData.utm_content = clickData.utm_content;
            baseSaleData.utm_term = clickData.utm_term;
        }

        console.log(`🎯 UTMs finais para a venda:`);
        console.log(`- source: ${baseSaleData.utm_source}`);
        console.log(`- campaign: ${baseSaleData.utm_campaign}`);
        console.log(`- medium: ${baseSaleData.utm_medium}`);

        // 6️⃣ Salvar no banco
        const saveResult = await saveSale({
            ...baseSaleData,
            approved_at: baseSaleData.approved_at
        });

        console.log("💾 VENDA SALVA/ATUALIZADA:", saleCode);
        console.log(`- ID no banco: ${saveResult.id}`);
        console.log(`- Valor salvo: R$ ${baseSaleData.plan_value}`);

        // 7️⃣ Enviar para UTMify
        console.log("\n📤 ENVIANDO PARA UTMIFY...");
        const utmRes = await sendToUtmify(baseSaleData, clickData);
        console.log("📤 UTMIFY RESULTADO:");
        console.log(`- Sucesso: ${utmRes.success}`);
        if (utmRes.error) {
            console.log(`- Erro: ${utmRes.error}`);
        }

        // 8️⃣ Enviar para TikTok/Facebook somente se approved
        if (baseSaleData.status === "approved") {
            console.log("\n📣 Enviando eventos de pixel...");
            const pixelResults = await processPixelEvents(baseSaleData, clickData, false);

            console.log("🎯 Resultados dos Pixels:");
            pixelResults.forEach(result => {
                console.log(`- ${result.platform}: ${result.success ? '✅' : '❌'} ${result.error || 'Sucesso'}`);
            });
        } else {
            console.log(`⏸️ Status não é 'approved' (${baseSaleData.status}), pulando pixels`);
        }

        console.log("\n✅ EVENTO APEX PROCESSADO COM SUCESSO!");
        console.log(`📊 Resumo:`);
        console.log(`- Código: ${saleCode}`);
        console.log(`- Valor: R$ ${baseSaleData.plan_value.toFixed(2)}`);
        console.log(`- Status: ${baseSaleData.status}`);
        console.log(`- UTM Source: ${baseSaleData.utm_source}`);
        console.log(`- UTMify: ${utmRes.success ? '✅ Enviado' : '❌ Falhou'}`);

    } catch (err) {
        console.error("\n❌ ERRO DETALHADO NO PROCESSAMENTO APEX:");
        console.error("Mensagem:", err.message);
        console.error("Stack:", err.stack);

        if (err.response) {
            console.error("Response data:", err.response.data);
            console.error("Response status:", err.response.status);
        }
    }
}

// Função para buscar click por múltiplos critérios
async function findClickByMultipleCriteria(clickId) {
    if (!clickId) return null;

    console.log(`🔍 Busca avançada por click: "${clickId}"`);

    try {
        const queries = [
            // Todas com LIMIT 1
            { query: 'SELECT * FROM clicks WHERE click_id = $1 LIMIT 1', params: [clickId] },
            { query: 'SELECT * FROM clicks WHERE utm_id = $1 LIMIT 1', params: [clickId] },
            { query: 'SELECT * FROM clicks WHERE click_id LIKE $1 LIMIT 1', params: [`%${clickId}%`] },
            { query: 'SELECT * FROM clicks WHERE utm_content LIKE $1 LIMIT 1', params: [`%${clickId}%`] },
            {
                query: `SELECT c.* FROM clicks c 
                     JOIN sales s ON c.click_id = s.click_id 
                     WHERE s.sale_code = $1 LIMIT 1`,
                params: [clickId]
            }
        ];

        for (const { query, params } of queries) {
            const result = await pool.query(query, params);
            if (result.rows.length > 0) {
                console.log(`✅ Encontrado via query: ${query.substring(0, 50)}...`);
                return result.rows[0];
            }
        }

        return null;

    } catch (error) {
        console.error('❌ Erro na busca avançada:', error.message);
        return null;
    }
}

app.post('/admin/cleanup-test-data', async (req, res) => {
    try {
        // Remover vendas de teste
        const deleteTestSales = await pool.query(
            "DELETE FROM sales WHERE sale_code LIKE 'TEST_%' OR sale_code LIKE 'APEX_%'"
        );

        // Remover clicks de teste
        const deleteTestClicks = await pool.query(
            "DELETE FROM clicks WHERE click_id LIKE 'test_%' OR click_id LIKE 'tg_%' OR click_id LIKE 'pixel_%'"
        );

        res.json({
            success: true,
            message: 'Dados de teste removidos',
            deleted: {
                sales: deleteTestSales.rowCount,
                clicks: deleteTestClicks.rowCount
            }
        });

    } catch (error) {
        console.error('❌ Erro na limpeza:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Atualizar a rota /redirect
app.get('/redirect', async (req, res) => {
    try {
        const { click_id, url, ...params } = req.query;

        // CORREÇÃO: Normalizar click_id do Telegram + limite de caracteres
        let normalizedClickId = click_id;
        if (click_id) {
            // Remove caracteres especiais que o Telegram pode cortar
            normalizedClickId = click_id.replace(/[^a-zA-Z0-9_\-]/g, '');

            // 🔥 CORREÇÃO: Limitar a 64 caracteres (limite seguro para DB)
            if (normalizedClickId.length > 64) {
                normalizedClickId = normalizedClickId.slice(0, 64);
                console.log(`⚠️ Click_id truncado para 64 caracteres: ${normalizedClickId}`);
            }

            // Se ficou vazio, gera um novo
            if (!normalizedClickId || normalizedClickId.length < 3) {
                normalizedClickId = `tg_${Date.now()}`;
                if (normalizedClickId.length > 64) {
                    normalizedClickId = normalizedClickId.slice(0, 64);
                }
            }
        }

        let destination = url || TELEGRAM_BOT_URL;

        if ((destination.includes('t.me') || destination.includes('telegram.me')) && normalizedClickId) {
            const urlObj = new URL(destination);
            urlObj.searchParams.set('start', normalizedClickId);
            destination = urlObj.toString();
        }

        if (normalizedClickId) {
            const clickData = {
                click_id: normalizedClickId,
                timestamp_ms: Date.now(),
                ip: req.ip || req.headers['x-forwarded-for'],
                user_agent: req.headers['user-agent'],
                referrer: req.headers['referer'] || req.headers['referrer'],
                utm_source: params.utm_source || params.us,
                utm_medium: params.utm_medium || params.um,
                utm_campaign: params.utm_campaign || params.uc,
                ttclid: params.ttclid,
                fbclid: params.fbclid
            };

            saveClick(clickData).catch(console.error);
        }

        res.redirect(302, destination);

    } catch (error) {
        console.error('❌ Erro no redirect:', error.message);
        res.redirect(302, TELEGRAM_BOT_URL);
    }
});

async function recoverUTM(sale) {
    console.log(`🔍 RECOVER UTM para: ${sale.sale_code} / ${sale.click_id}`);

    // 1. Buscar por fbc/fbp (Facebook dedupe) - NOVA PRIORIDADE
    if (sale.fbc || sale.fbp) {
        const resFB = await pool.query(
            `SELECT * FROM clicks 
             WHERE (fbc = $1 OR fbp = $2)
             AND (fbc IS NOT NULL OR fbp IS NOT NULL)
             ORDER BY received_at DESC 
             LIMIT 1`,
            [sale.fbc || '', sale.fbp || '']
        );
        if (resFB.rows.length > 0) {
            console.log(`✅ Click encontrado por fbc/fbp (Facebook dedupe)`);
            return resFB.rows[0];
        }
    }

    // 2. Buscar por ttclid primeiro (mais preciso para TikTok)
    if (sale.click_id) {
        const resTTCLID = await pool.query(
            `SELECT * FROM clicks 
             WHERE click_id = $1 
             AND ttclid IS NOT NULL 
             AND ttclid != ''
             LIMIT 1`,
            [sale.click_id]
        );
        if (resTTCLID.rows.length > 0) {
            console.log(`✅ Click encontrado por click_id com ttclid: ${sale.click_id}`);
            return resTTCLID.rows[0];
        }
    }

    // 3. Tentar com click_id exato
    if (sale.click_id) {
        const res1 = await pool.query(
            "SELECT * FROM clicks WHERE click_id = $1 LIMIT 1",
            [sale.click_id]
        );
        if (res1.rows.length > 0) {
            console.log(`✅ Click encontrado por click_id: ${sale.click_id}`);
            return res1.rows[0];
        }
    }

    // 4. Tentar com sale_code
    const res2 = await pool.query(
        "SELECT * FROM clicks WHERE click_id = $1 LIMIT 1",
        [sale.sale_code]
    );
    if (res2.rows.length > 0) {
        console.log(`✅ Click encontrado por sale_code: ${sale.sale_code}`);
        return res2.rows[0];
    }

    // 5. Buscar cliques recentes do mesmo IP (última hora) com prioridade para ttclid
    if (sale.ip && sale.ip !== '0.0.0.0') {
        const res3 = await pool.query(
            `SELECT * FROM clicks 
             WHERE ip = $1 
             AND received_at >= NOW() - INTERVAL '1 hour'
             ORDER BY 
                 CASE WHEN ttclid IS NOT NULL AND ttclid != '' THEN 1 
                      WHEN fbc IS NOT NULL OR fbp IS NOT NULL THEN 2
                      ELSE 3 END,
                 received_at DESC 
             LIMIT 1`,
            [sale.ip]
        );
        if (res3.rows.length > 0) {
            console.log(`✅ Click encontrado por IP recente: ${sale.ip}`);
            return res3.rows[0];
        }
    }

    console.log(`❌ Nenhum click encontrado para ${sale.sale_code}`);
    return null;
}

// --- ROTAS DE ADMIN ---

// Listar pixels
app.get('/admin/pixels', async (req, res) => {
    try {
        const pixels = await getActivePixels();
        res.json(pixels);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Adicionar esta nova rota de teste específica para Facebook
app.post('/api/test/facebook-value', async (req, res) => {
    try {
        const { pixel_id, customer_email, plan_value = 97.00 } = req.body;

        console.log('🧪 Teste específico para Facebook (valor em reais)');

        // Buscar pixel Facebook
        const pixels = await pool.query(
            'SELECT * FROM pixels WHERE platform = $1 AND is_active = TRUE',
            ['facebook']
        );

        if (!pixels.rows || pixels.rows.length === 0) {
            return res.status(404).json({
                success: false,
                error: 'Nenhum pixel Facebook ativo encontrado'
            });
        }

        const pixel = pixels.rows[0];

        const testData = {
            sale_code: `FB_TEST_${Date.now()}`,
            customer_email: customer_email || 'test@example.com',
            customer_phone: '11999999999',
            customer_document: '12345678900',
            customer_name: 'Cliente Teste Facebook',
            plan_name: 'Acesso VIP Teste',
            plan_value: plan_value,
            currency: 'BRL',
            ip: '189.45.210.130',
            user_agent: 'Test-Facebook/1.0'
        };

        const clickData = {
            fbc: 'fb.1.1234567890.ABCDEF',
            fbp: 'fb.1.1234567890.ABCDEF',
            landing_page: 'https://teste.tracking.com'
        };

        const result = await sendFacebookEvent(pixel, testData, clickData, true);

        res.json({
            success: result.success,
            message: 'Teste Facebook executado',
            test_data: {
                ...testData,
                plan_value_formatted: `R$ ${parseFloat(testData.plan_value).toFixed(2)}`
            },
            result: {
                valor_enviado: parseFloat(testData.plan_value).toFixed(2),
                valor_em_centavos: (testData.plan_value * 100),
                observacao: 'Facebook deve receber valor EM REAIS, não centavos!',
                facebook_response: result.data,
                error: result.error
            }
        });

    } catch (error) {
        console.error('❌ Erro no teste Facebook:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/admin/resend-utmify', async (req, res) => {
    try {
        const { only_missing = false, limit = null } = req.body || {};

        let query = "SELECT * FROM sales";
        if (only_missing) query += " WHERE utmify_sent = FALSE";
        query += " ORDER BY created_at ASC";
        if (limit && Number.isInteger(limit)) query += ` LIMIT ${limit}`;

        const result = await pool.query(query);
        const sales = result.rows;

        const summary = { total: sales.length, success: 0, failed: 0, details: [] };

        for (const sale of sales) {
            try {
                const clickData = await recoverUTM(sale);

                const saleData = {
                    sale_code: sale.sale_code,
                    click_id: sale.click_id || sale.sale_code,
                    customer_name: sale.customer_name,
                    customer_email: sale.customer_email,
                    customer_phone: sale.customer_phone,
                    customer_document: sale.customer_document,
                    plan_name: sale.plan_name,
                    plan_value: sale.plan_value,
                    currency: sale.currency,
                    payment_platform: sale.payment_platform,
                    payment_method: sale.payment_method,
                    ip: sale.ip,
                    user_agent: sale.user_agent,

                    utm_source: clickData?.utm_source,
                    utm_medium: clickData?.utm_medium,
                    utm_campaign: clickData?.utm_campaign,
                    utm_content: clickData?.utm_content,
                    utm_term: clickData?.utm_term,

                    status: sale.status,
                    created_at: sale.created_at,
                    approved_at: sale.approved_at
                };

                const utmRes = await sendToUtmify(saleData, clickData);

                await processPixelEvents(saleData, clickData, false);

                summary.details.push({
                    sale_code: sale.sale_code,
                    utmify: utmRes.success,
                });

                if (utmRes.success) summary.success++;
                else summary.failed++;

            } catch (err) {
                summary.failed++;
            }
        }

        res.json({ success: true, summary });

    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

app.get('/admin/pixels/:id', async (req, res) => {
    try {
        const pixelId = parseInt(req.params.id);

        if (isNaN(pixelId) || pixelId <= 0) {
            return res.status(400).json({ error: 'ID inválido' });
        }

        const result = await pool.query(
            'SELECT * FROM pixels WHERE id = $1',
            [pixelId]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Pixel não encontrado' });
        }

        // Não retornar o access_token por segurança
        const pixel = result.rows[0];
        const safePixel = {
            id: pixel.id,
            name: pixel.name,
            platform: pixel.platform,
            pixel_id: pixel.pixel_id,
            event_source_id: pixel.event_source_id,
            test_event_code: pixel.test_event_code,
            is_active: pixel.is_active,
            created_at: pixel.created_at,
            updated_at: pixel.updated_at
        };

        res.json(safePixel);

    } catch (error) {
        console.error('❌ Erro ao buscar pixel:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Atualizar pixel
app.put('/admin/pixels/:id', async (req, res) => {
    try {
        const pixelId = parseInt(req.params.id);
        const { name, platform, pixel_id, event_source_id, access_token, test_event_code, is_active } = req.body;

        if (isNaN(pixelId) || pixelId <= 0) {
            return res.status(400).json({ error: 'ID inválido' });
        }

        // Verificar se o pixel existe
        const checkResult = await pool.query(
            'SELECT * FROM pixels WHERE id = $1',
            [pixelId]
        );

        if (checkResult.rows.length === 0) {
            return res.status(404).json({ error: 'Pixel não encontrado' });
        }

        const currentPixel = checkResult.rows[0];

        // CORREÇÃO: Não permitir trocar plataforma
        if (platform !== undefined && platform !== currentPixel.platform) {
            return res.status(400).json({
                error: 'Não é permitido alterar a plataforma do pixel',
                current_platform: currentPixel.platform,
                requested_platform: platform
            });
        }

        // Construir query dinâmica
        const updates = [];
        const values = [];
        let paramCount = 1;

        if (name !== undefined) {
            updates.push(`name = $${paramCount}`);
            values.push(name);
            paramCount++;
        }

        // platform não é mais atualizável aqui
        if (pixel_id !== undefined) {
            updates.push(`pixel_id = $${paramCount}`);
            values.push(pixel_id);
            paramCount++;
        }

        if (event_source_id !== undefined) {
            updates.push(`event_source_id = $${paramCount}`);
            values.push(event_source_id);
            paramCount++;
        }

        if (access_token !== undefined) {
            updates.push(`access_token = $${paramCount}`);
            values.push(access_token);
            paramCount++;
        }

        if (test_event_code !== undefined) {
            updates.push(`test_event_code = $${paramCount}`);
            values.push(test_event_code);
            paramCount++;
        }

        if (is_active !== undefined) {
            updates.push(`is_active = $${paramCount}`);
            values.push(is_active);
            paramCount++;
        }

        if (updates.length === 0) {
            return res.status(400).json({ error: 'Nenhum campo para atualizar' });
        }

        updates.push(`updated_at = CURRENT_TIMESTAMP`);
        values.push(pixelId);

        const query = `
            UPDATE pixels 
            SET ${updates.join(', ')}
            WHERE id = $${paramCount}
            RETURNING *;
        `;

        const result = await pool.query(query, values);
        res.json({
            success: true,
            message: 'Pixel atualizado com sucesso',
            pixel: result.rows[0]
        });

    } catch (error) {
        console.error('❌ Erro ao atualizar pixel:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Deletar pixel
app.delete('/admin/pixels/:id', async (req, res) => {
    try {
        const pixelId = parseInt(req.params.id);

        if (isNaN(pixelId) || pixelId <= 0) {
            return res.status(400).json({ error: 'ID inválido' });
        }

        // Verificar se o pixel existe
        const checkResult = await pool.query(
            'SELECT * FROM pixels WHERE id = $1',
            [pixelId]
        );

        if (checkResult.rows.length === 0) {
            return res.status(404).json({ error: 'Pixel não encontrado' });
        }

        // Soft delete (marcar como inativo) - preferível para manter histórico
        const result = await pool.query(
            'UPDATE pixels SET is_active = FALSE WHERE id = $1 RETURNING *',
            [pixelId]
        );

        console.log(`🗑️ Pixel deletado: ${result.rows[0].name} (${result.rows[0].platform})`);

        res.json({
            success: true,
            message: 'Pixel deletado com sucesso',
            pixel: result.rows[0]
        });

    } catch (error) {
        console.error('❌ Erro ao deletar pixel:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Rota alternativa para múltipla exclusão
app.post('/admin/pixels/delete', async (req, res) => {
    try {
        const { ids } = req.body;

        if (!ids || !Array.isArray(ids) || ids.length === 0) {
            return res.status(400).json({ error: 'IDs inválidos' });
        }

        // Converter para números
        const pixelIds = ids.map(id => parseInt(id)).filter(id => !isNaN(id) && id > 0);

        if (pixelIds.length === 0) {
            return res.status(400).json({ error: 'Nenhum ID válido fornecido' });
        }

        // Soft delete
        const result = await pool.query(
            'UPDATE pixels SET is_active = FALSE WHERE id = ANY($1) RETURNING *',
            [pixelIds]
        );

        console.log(`🗑️ ${result.rowCount} pixels deletados`);

        res.json({
            success: true,
            message: `${result.rowCount} pixel(s) deletado(s) com sucesso`,
            deleted_count: result.rowCount,
            pixels: result.rows
        });

    } catch (error) {
        console.error('❌ Erro ao deletar múltiplos pixels:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Adicionar/atualizar pixel
app.post('/admin/pixels', async (req, res) => {
    try {
        const { name, platform, pixel_id, event_source_id, access_token, test_event_code } = req.body;

        if (!name || !platform || !pixel_id || !access_token) {
            return res.status(400).json({ error: 'Dados incompletos' });
        }

        const query = `
            INSERT INTO pixels (name, platform, pixel_id, event_source_id, access_token, test_event_code)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (platform, pixel_id) DO UPDATE SET
                name = EXCLUDED.name,
                event_source_id = EXCLUDED.event_source_id,
                access_token = EXCLUDED.access_token,
                test_event_code = EXCLUDED.test_event_code,
                is_active = TRUE
            RETURNING *;
        `;

        const result = await pool.query(query, [
            name, platform, pixel_id, event_source_id || pixel_id,
            access_token, test_event_code || null
        ]);

        res.json({ success: true, pixel: result.rows[0] });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Estatísticas completas
app.get('/admin/stats', async (req, res) => {
    try {
        const [clicks, salesStats, revenueStats, pixels, funnel, recentSales, topCampaigns] = await Promise.all([
            // Total de cliques
            pool.query('SELECT COUNT(*) as count FROM clicks'),

            // Estatísticas de vendas por status
            pool.query(`
                SELECT 
                    status,
                    COUNT(*) as count,
                    SUM(plan_value) as total_value
                FROM sales 
                GROUP BY status
                ORDER BY 
                    CASE status 
                        WHEN 'approved' THEN 1
                        WHEN 'created' THEN 2
                        WHEN 'pending' THEN 3
                        ELSE 4
                    END
            `),

            // Receita total (aprovada)
            pool.query(`
                SELECT 
                    SUM(plan_value) as total_approved,
                    COUNT(*) as count_approved,
                    AVG(plan_value) as avg_ticket
                FROM sales 
                WHERE status = 'approved'
            `),

            // Pixels ativos
            pool.query('SELECT COUNT(*) as count FROM pixels WHERE is_active = TRUE'),

            // Funil de conversão (últimos 30 dias)
            pool.query(`
                WITH dates AS (
                    SELECT generate_series(
                        CURRENT_DATE - INTERVAL '30 days', 
                        CURRENT_DATE, 
                        '1 day'::interval
                    )::date as date
                ),
                daily_clicks AS (
                    SELECT 
                        DATE(received_at) as date,
                        COUNT(*) as clicks
                    FROM clicks
                    WHERE received_at >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY DATE(received_at)
                ),
                daily_sales AS (
                    SELECT 
                        DATE(created_at) as date,
                        COUNT(*) as sales,
                        SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved_sales
                    FROM sales
                    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY DATE(created_at)
                )
                SELECT 
                    d.date,
                    COALESCE(dc.clicks, 0) as clicks,
                    COALESCE(ds.sales, 0) as sales,
                    COALESCE(ds.approved_sales, 0) as approved_sales,
                    CASE 
                        WHEN COALESCE(dc.clicks, 0) > 0 
                        THEN ROUND((COALESCE(ds.sales, 0) * 100.0 / dc.clicks), 2)
                        ELSE 0
                    END as conversion_rate
                FROM dates d
                LEFT JOIN daily_clicks dc ON d.date = dc.date
                LEFT JOIN daily_sales ds ON d.date = ds.date
                ORDER BY d.date DESC
                LIMIT 30
            `),

            // Vendas recentes (últimas 10)
            pool.query(`
                SELECT 
                    sale_code,
                    customer_name,
                    customer_email,
                    plan_name,
                    plan_value,
                    status,
                    created_at,
                    utm_source,
                    utm_campaign
                FROM sales
                ORDER BY created_at DESC
                LIMIT 10
            `),

            // Top campanhas por conversão (CORREÇÃO COMPLETA)
            pool.query(`
                SELECT 
                    COALESCE(c.utm_source, s.utm_source) as utm_source,
                    COALESCE(c.utm_campaign, s.utm_campaign) as utm_campaign,
                    COUNT(DISTINCT s.sale_code) as sales_count,
                    SUM(s.plan_value) as total_revenue,
                    COUNT(DISTINCT c.click_id) as clicks_count,
                    CASE 
                        WHEN COUNT(DISTINCT c.click_id) > 0 
                        THEN ROUND((COUNT(DISTINCT s.sale_code) * 100.0 / COUNT(DISTINCT c.click_id)), 2)
                        ELSE 0
                    END as conversion_rate
                FROM sales s
                LEFT JOIN clicks c ON (
                    s.click_id = c.click_id 
                    OR s.sale_code = c.click_id
                    OR (c.utm_id IS NOT NULL AND c.utm_id = s.sale_code)
                )
                WHERE (c.utm_source IS NOT NULL OR s.utm_source IS NOT NULL)
                    AND s.created_at >= CURRENT_DATE - INTERVAL '30 days'
                    AND s.status = 'approved'
                GROUP BY 
                    COALESCE(c.utm_source, s.utm_source),
                    COALESCE(c.utm_campaign, s.utm_campaign)
                HAVING COUNT(DISTINCT s.sale_code) > 0
                ORDER BY total_revenue DESC
                LIMIT 10
            `)
        ]);

        // Processar estatísticas de vendas
        const salesByStatus = salesStats.rows.reduce((acc, row) => {
            acc[row.status] = {
                count: parseInt(row.count),
                value: parseFloat(row.total_value || 0)
            };
            return acc;
        }, {});

        // Processar funil
        const funnelData = funnel.rows.map(row => ({
            date: row.date.toISOString().split('T')[0],
            clicks: parseInt(row.clicks),
            sales: parseInt(row.sales),
            approved_sales: parseInt(row.approved_sales),
            conversion_rate: parseFloat(row.conversion_rate)
        }));

        // Calcular totais do funil
        const funnelTotals = funnel.rows.reduce((acc, row) => ({
            clicks: acc.clicks + parseInt(row.clicks),
            sales: acc.sales + parseInt(row.sales),
            approved_sales: acc.approved_sales + parseInt(row.approved_sales)
        }), { clicks: 0, sales: 0, approved_sales: 0 });

        // Calcular taxa de conversão geral
        const overallConversion = funnelTotals.clicks > 0
            ? parseFloat(((funnelTotals.sales * 100) / funnelTotals.clicks).toFixed(2))
            : 0;

        const approvedConversion = funnelTotals.clicks > 0
            ? parseFloat(((funnelTotals.approved_sales * 100) / funnelTotals.clicks).toFixed(2))
            : 0;

        res.json({
            // Totais básicos
            totals: {
                clicks: parseInt(clicks.rows[0].count),
                active_pixels: parseInt(pixels.rows[0].count),
                utmify_configured: !!UTMIFY_API_KEY
            },

            // Estatísticas de vendas
            sales: {
                by_status: salesByStatus,
                total_approved: parseFloat(revenueStats.rows[0].total_approved || 0).toFixed(2),
                count_approved: parseInt(revenueStats.rows[0].count_approved || 0),
                avg_ticket: parseFloat(revenueStats.rows[0].avg_ticket || 0).toFixed(2),
                pending: salesByStatus['pending']?.count || 0,
                created: salesByStatus['created']?.count || 0,
                approved: salesByStatus['approved']?.count || 0
            },

            // Funil de conversão
            funnel: {
                daily: funnelData,
                totals: funnelTotals,
                conversion: {
                    overall: overallConversion,
                    approved: approvedConversion,
                    clicks_to_sales: funnelTotals.clicks > 0 ? funnelTotals.sales : 0,
                    clicks_to_approved: funnelTotals.clicks > 0 ? funnelTotals.approved_sales : 0
                }
            },

            // Vendas recentes
            recent_sales: recentSales.rows.map(row => ({
                sale_code: row.sale_code,
                customer_name: row.customer_name,
                customer_email: row.customer_email ?
                    row.customer_email.replace(/(.{2})(.*)(@.*)/, '$1***$3') : 'N/A',
                plan_name: row.plan_name,
                plan_value: parseFloat(row.plan_value).toFixed(2),
                status: row.status,
                created_at: row.created_at,
                utm_source: row.utm_source || 'N/A',
                utm_campaign: row.utm_campaign || 'N/A'
            })),

            // Top campanhas
            top_campaigns: topCampaigns.rows.map(row => ({
                source: row.utm_source || 'N/A',
                campaign: row.utm_campaign || 'N/A',
                sales_count: parseInt(row.sales_count),
                total_revenue: parseFloat(row.total_revenue).toFixed(2),
                clicks_count: parseInt(row.clicks_count),
                conversion_rate: parseFloat(row.conversion_rate),
                // Adicionar insights
                cpa: row.clicks_count > 0 ? (parseFloat(row.total_revenue) / parseInt(row.clicks_count)).toFixed(2) : 'N/A',
                roas: row.clicks_count > 0 ? (parseFloat(row.total_revenue) / (parseInt(row.clicks_count) * 0.5)).toFixed(2) : 'N/A' // Assumindo CPC de R$0,50
            })),

            // Timestamp
            generated_at: new Date().toISOString(),
            period: 'last_30_days',
            // Adicionar metadados
            metadata: {
                timezone: 'UTC',
                currency: 'BRL',
                funnel_window: '30_days'
            }
        });

    } catch (error) {
        console.error('❌ Erro ao buscar estatísticas:', error.message);
        res.status(500).json({
            error: error.message,
            details: 'Verifique a conexão com o banco de dados'
        });
    }
});

// Estatísticas rápidas (para dashboard)
app.get('/admin/stats/quick', async (req, res) => {
    try {
        const [today, yesterday, week] = await Promise.all([
            // Hoje
            pool.query(`
                SELECT 
                    (SELECT COUNT(*) FROM clicks WHERE DATE(received_at) = CURRENT_DATE) as clicks_today,
                    (SELECT COUNT(*) FROM sales WHERE DATE(created_at) = CURRENT_DATE AND status = 'approved') as sales_today,
                    (SELECT SUM(plan_value) FROM sales WHERE DATE(created_at) = CURRENT_DATE AND status = 'approved') as revenue_today
            `),

            // Ontem
            pool.query(`
                SELECT 
                    (SELECT COUNT(*) FROM clicks WHERE DATE(received_at) = CURRENT_DATE - INTERVAL '1 day') as clicks_yesterday,
                    (SELECT COUNT(*) FROM sales WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1 day' AND status = 'approved') as sales_yesterday,
                    (SELECT SUM(plan_value) FROM sales WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1 day' AND status = 'approved') as revenue_yesterday
            `),

            // Últimos 7 dias
            pool.query(`
                SELECT 
                    COUNT(*) as clicks_week,
                    (SELECT COUNT(*) FROM sales WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' AND status = 'approved') as sales_week,
                    (SELECT SUM(plan_value) FROM sales WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' AND status = 'approved') as revenue_week
                FROM clicks 
                WHERE received_at >= CURRENT_DATE - INTERVAL '7 days'
            `)
        ]);

        const todayData = today.rows[0];
        const yesterdayData = yesterday.rows[0];
        const weekData = week.rows[0];

        // Calcular crescimento
        const salesGrowth = yesterdayData.sales_yesterday > 0 ?
            parseFloat(((todayData.sales_today - yesterdayData.sales_yesterday) * 100 / yesterdayData.sales_yesterday).toFixed(1)) : 0;

        const revenueGrowth = yesterdayData.revenue_yesterday > 0 ?
            parseFloat(((todayData.revenue_today - yesterdayData.revenue_yesterday) * 100 / yesterdayData.revenue_yesterday).toFixed(1)) : 0;

        res.json({
            today: {
                clicks: parseInt(todayData.clicks_today || 0),
                sales: parseInt(todayData.sales_today || 0),
                revenue: parseFloat(todayData.revenue_today || 0).toFixed(2)
            },
            yesterday: {
                clicks: parseInt(yesterdayData.clicks_yesterday || 0),
                sales: parseInt(yesterdayData.sales_yesterday || 0),
                revenue: parseFloat(yesterdayData.revenue_yesterday || 0).toFixed(2)
            },
            week: {
                clicks: parseInt(weekData.clicks_week || 0),
                sales: parseInt(weekData.sales_week || 0),
                revenue: parseFloat(weekData.revenue_week || 0).toFixed(2)
            },
            growth: {
                sales: salesGrowth,
                revenue: revenueGrowth
            },
            generated_at: new Date().toISOString()
        });

    } catch (error) {
        console.error('❌ Erro ao buscar estatísticas rápidas:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Rota 6: Teste manual da API de eventos (para usar no admin.html)
app.post('/api/test/events', async (req, res) => {
    try {
        const {
            platform,
            pixel_id,
            test_event_code = 'TEST54815',
            customer_email,
            customer_phone,
            plan_value = 97.00,
            ...otherParams
        } = req.body;

        console.log('🧪 Teste manual da API de eventos recebido');

        // Verificar se é teste de pixel específico
        let pixels;
        if (platform && pixel_id) {
            pixels = await pool.query(
                'SELECT * FROM pixels WHERE platform = $1 AND pixel_id = $2 AND is_active = TRUE',
                [platform, pixel_id]
            );
            pixels = pixels.rows;
        } else if (platform) {
            pixels = await getActivePixels(platform);
        } else {
            pixels = await getActivePixels();
        }

        if (!pixels || pixels.length === 0) {
            return res.status(404).json({
                success: false,
                error: 'Nenhum pixel ativo encontrado',
                hint: 'Configure pixels em /admin/pixels primeiro'
            });
        }

        const testData = {
            sale_code: `TEST_${Date.now()}`,
            customer_email: customer_email || 'test@example.com',
            customer_phone: customer_phone || '11999999999',
            customer_document: '12345678900',
            customer_name: 'Cliente Teste',
            plan_name: 'Acesso VIP Teste',
            plan_value: plan_value,
            currency: 'BRL',
            utm_source: 'test_source',
            utm_medium: 'test_medium',
            utm_campaign: 'test_campaign',
            utm_term: 'teste manual',
            ip: req.ip || '189.45.210.130',
            user_agent: req.headers['user-agent'] || 'Test-API/1.0'
        };

        const clickData = {
            landing_page: 'https://teste.tracking.com',
            referrer: 'https://facebook.com/test',
            ttclid: 'test_ttclid_123',
            fbclid: 'test_fbclid_456'
        };

        const results = [];
        for (const pixel of pixels) {
            try {
                let result;

                if (pixel.platform === 'tiktok') {
                    result = await sendTikTokEvent(pixel, testData, clickData, true); // true = teste
                } else if (pixel.platform === 'facebook') {
                    result = await sendFacebookEvent(pixel, testData, clickData, true); // true = teste
                }

                results.push({
                    platform: pixel.platform,
                    pixel_id: pixel.pixel_id,
                    name: pixel.name,
                    test_event_code: pixel.test_event_code || test_event_code,
                    success: result?.success || false,
                    data: result?.data,
                    error: result?.error
                });

            } catch (error) {
                console.error(`❌ Erro no teste do pixel ${pixel.platform}:`, error.message);
                results.push({
                    platform: pixel.platform,
                    pixel_id: pixel.pixel_id,
                    name: pixel.name,
                    success: false,
                    error: error.message,
                    response: error.response?.data
                });
            }
        }

        res.json({
            success: true,
            message: 'Teste de eventos executado',
            test_data: testData,
            test_event_code: test_event_code,
            results: results
        });

    } catch (error) {
        console.error('❌ Erro no teste de eventos:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Rota 7 para testar UTMify com diferentes status
app.post('/api/test/utmify-status', async (req, res) => {
    try {
        const { status = 'created' } = req.body; // 'created' ou 'approved'

        const testData = {
            sale_code: `TEST_${status.toUpperCase()}_${Date.now()}`,
            customer_name: 'Cliente Teste ' + status,
            customer_email: `teste_${status}@utmify.com`,
            customer_phone: '11999999999',
            plan_name: 'Acesso VIP Teste',
            plan_value: 97.00,
            currency: 'BRL',
            payment_platform: 'ApexVips',
            payment_method: 'pix',
            utm_source: 'facebook_test',
            utm_campaign: 'test_' + status,
            status: status,
            created_at: Math.floor(Date.now() / 1000),
            approved_at: status === 'approved' ? Math.floor(Date.now() / 1000) : null
        };

        const clickData = {
            ip: '189.45.210.130',
            user_agent: 'Test-UA/1.0',
            utm_source: 'facebook_test'
        };

        const result = await sendToUtmify(testData, clickData);

        res.json({
            success: result.success,
            message: `Teste ${status} enviado para UTMify`,
            status: status,
            data: result.data,
            error: result.error
        });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Rota para teste rápido
app.get('/test', async (req, res) => {
    try {
        const testClick = await saveClick({
            click_id: `test_${Date.now()}`,
            utm_source: 'test',
            utm_campaign: 'test_campaign',
            ttclid: 'test_ttclid'
        });

        res.json({
            success: true,
            message: 'Teste OK',
            click_saved: testClick.success,
            database: 'connected',
            env: {
                has_utmify_key: !!UTMIFY_API_KEY,
                telegram_url: TELEGRAM_BOT_URL
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// --- INICIALIZAÇÃO ---
async function startServer() {
    try {
        // Configurar banco
        await setupDatabase();

        // Auto-ping para manter Render ativo
        if (process.env.RENDER_EXTERNAL_URL) {
            setInterval(async () => {
                try {
                    await axios.get(`${process.env.RENDER_EXTERNAL_URL}/`);
                    console.log('💚 Auto-ping executado');
                } catch (error) {
                    console.error('❌ Auto-ping falhou:', error.message);
                }
            }, 5 * 60 * 1000); // 5 minutos
        }

        // Iniciar servidor
        app.listen(PORT, () => {
            console.log(`
🚀 Servidor iniciado na porta ${PORT}
🌐 URL: ${process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`}
🔑 UTMify: ${UTMIFY_API_KEY ? '✅ Configurada' : '❌ Não configurada'}
🤖 Telegram: ${TELEGRAM_BOT_URL}
🖥️ Criador: @gustavo.mcruz
📊 Endpoints:
   GET  /                   - Health check
   GET  /test               - Teste rápido
   POST /api/track          - Receber cliques
   GET  /pixel.gif          - Pixel tracking
   POST /api/webhook/apex   - Webhook Apex Vips
   GET  /redirect           - Redirecionamento
   POST /api/test/events    - Testar API de eventos (para admin.html)
   GET  /admin/pixels       - Listar pixels
   POST /admin/pixels       - Adicionar pixel
   GET  /admin/stats        - Estatísticas
            `);
        });

    } catch (error) {
        console.error('❌ Falha ao iniciar servidor:', error);
        process.exit(1);
    }
}

startServer();