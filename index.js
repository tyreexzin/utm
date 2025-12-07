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

// --- FUNÇÕES AUXILIARES ---
function hashData(data) {
    if (!data || typeof data !== 'string') return null;
    return crypto.createHash('sha256')
        .update(data.toLowerCase().trim())
        .digest('hex');
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
            plan_value = COALESCE(EXCLUDED.plan_value, sales.plan_value)
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
        data.plan_value || 0,
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

// --- FUNÇÃO UTMIFY ---
async function sendToUtmify(saleData, clickData) {
    if (!UTMIFY_API_KEY) {
        console.log('⚠️ UTMIFY_API_KEY não configurada');
        return { success: false, error: 'API key não configurada' };
    }

    try {
        const now = new Date();
        const isTest = saleData.sale_code.includes('TEST') || false;

        // Status correto para UTMify
        let utmifyStatus = 'waiting_payment';
        if (saleData.status === 'approved' || saleData.status === 'paid') {
            utmifyStatus = 'paid';
        }

        // Gerar planId baseado no plan_name
        const planId = saleData.plan_name
            ? saleData.plan_name.toLowerCase()
                .replace(/[^a-z0-9]/g, '-')
                .replace(/-+/g, '-')
                .replace(/^-|-$/g, '')
                .substring(0, 50)
            : 'apex-access';

        // Determinar UTM parameters - usar clickData se disponível, senão usar saleData
        const utmSource = saleData.utm_source || clickData?.utm_source || 'direct';
        const utmMedium = saleData.utm_medium || clickData?.utm_medium || 'organic';
        const utmCampaign = saleData.utm_campaign || clickData?.utm_campaign || 'default';
        const utmContent = saleData.utm_content || clickData?.utm_content || '';
        const utmTerm = saleData.utm_term || clickData?.utm_term || '';

        console.log('📊 DETALHES DO ENVIO UTMIFY:');
        console.log('- Venda:', saleData.sale_code);
        console.log('- Cliente:', saleData.customer_name);
        console.log('- Valor:', saleData.plan_value);
        console.log('- Status UTMify:', utmifyStatus);
        console.log('- É teste?', isTest);
        console.log('📊 PARÂMETROS DE TRACKING:');
        console.log('- UTM Source:', utmSource);
        console.log('- UTM Medium:', utmMedium);
        console.log('- UTM Campaign:', utmCampaign);
        console.log('- UTM Content:', utmContent);
        console.log('- UTM Term:', utmTerm);
        console.log('- Click ID associado:', clickData?.click_id || 'Nenhum');
        console.log('- Landing Page original:', clickData?.landing_page || 'Não disponível');
        console.log('- Referrer:', clickData?.referrer || 'Não disponível');

        const payload = {
            orderId: saleData.sale_code,
            platform: saleData.payment_platform || 'ApexVips',
            paymentMethod: saleData.payment_method || 'unknown',
            status: utmifyStatus,
            createdAt: sale.created_at
                ? new Date(sale.created_at).toISOString().replace('T', ' ').substring(0, 19)
                : now.toISOString().replace('T', ' ').substring(0, 19),

            approvedDate: (sale.status === "approved" && sale.approved_at)
                ? new Date(sale.approved_at).toISOString().replace('T', ' ').substring(0, 19)
                : null,
            customer: {
                name: saleData.customer_name || 'Cliente',
                email: saleData.customer_email || "naoinformado@utmify.com",
                phone: saleData.customer_phone ? saleData.customer_phone.replace(/\D/g, '') : null,
                document: saleData.customer_document ? saleData.customer_document.replace(/\D/g, '') : null,
                ip: saleData.ip || clickData?.ip || '0.0.0.0'
            },
            products: [{
                id: planId,
                planId: planId,
                name: saleData.plan_name || 'Acesso Apex Vips',
                planName: saleData.plan_name || 'Acesso Apex Vips',
                quantity: 1,
                priceInCents: Math.round((saleData.plan_value || 0) * 100)
            }],
            trackingParameters: {
                utm_source: utmSource,
                utm_medium: utmMedium,
                utm_campaign: utmCampaign,
                utm_content: utmContent,
                utm_term: utmTerm
            },
            commission: {
                totalPriceInCents: Math.round((saleData.plan_value || 0) * 100),
                gatewayFeeInCents: 0,
                userCommissionInCents: Math.round((saleData.plan_value || 0) * 100),
                currency: saleData.currency || 'BRL'
            },
            isTest: isTest
        };

        console.log('📤 Enviando para UTMify:', JSON.stringify(payload, null, 2));

        const response = await axios.post(
            'https://api.utmify.com.br/api-credentials/orders',
            payload,
            {
                headers: {
                    'x-api-token': UTMIFY_API_KEY,
                    'Content-Type': 'application/json'
                },
                timeout: 10000
            }
        );

        // MELHORIA: Log completo da resposta
        console.log(`✅ UTMify Response (${response.status}):`, JSON.stringify(response.data, null, 2));
        console.log(`✅ UTMify: Venda ${saleData.sale_code} enviada (${utmifyStatus})`);

        // Marcar como enviado no banco
        await pool.query(
            'UPDATE sales SET utmify_sent = TRUE WHERE sale_code = $1',
            [saleData.sale_code]
        );

        return { success: true, data: response.data };

    } catch (error) {
        // MELHORIA: Log mais detalhado
        console.error('❌ Erro ao enviar para UTMify:', {
            status: error.response?.status,
            statusText: error.response?.statusText,
            data: error.response?.data,
            message: error.message,
            config: {
                url: error.config?.url,
                method: error.config?.method,
                data: error.config?.data ? JSON.parse(error.config.data) : null
            }
        });
        return { success: false, error: error.message };
    }
}

// --- FUNÇÕES DE PIXEL ---

// Enviar evento para TikTok (API v1.3 atualizada) - CORRIGIDA
async function sendTikTokEvent(pixel, eventData, clickData, isTest = false) {
    const url = 'https://business-api.tiktok.com/open_api/v1.3/event/track/';

    // Preparar dados do usuário (hashed)
    const user = {};
    if (eventData.customer_email) {
        user.email = hashData(eventData.customer_email);
    }
    if (eventData.customer_phone) {
        user.phone = hashData(eventData.customer_phone.replace(/\D/g, ''));
    }
    if (eventData.customer_document) {
        user.external_id = hashData(eventData.customer_document.replace(/\D/g, ''));
    }

    // Determinar content_type baseado no plan_name
    let content_type = 'product';
    if (eventData.plan_name) {
        const planLower = eventData.plan_name.toLowerCase();
        if (planLower.includes('curso') || planLower.includes('treinamento') || planLower.includes('mentoria')) {
            content_type = 'course';
        } else if (planLower.includes('consultoria') || planLower.includes('serviço') || planLower.includes('service')) {
            content_type = 'service';
        } else if (planLower.includes('assinatura') || planLower.includes('subscription')) {
            content_type = 'subscription';
        }
    }

    // CORREÇÃO: Corrigir o valor (multiplicar por 100 para centavos)
    const valueInCents = eventData.plan_value * 100;

    // Construir payload conforme documentação do TikTok
    const payload = {
        event_source: "web",
        event_source_id: pixel.event_source_id || pixel.pixel_id,
        data: [
            {
                event: "Purchase",
                event_time: Math.floor(Date.now() / 1000),
                event_id: eventData.sale_code,
                user: Object.keys(user).length > 0 ? user : undefined,
                properties: {
                    value: valueInCents, // CORREÇÃO: Valor em centavos
                    currency: eventData.currency || 'BRL',
                    content_id: 'vip_access',
                    content_type: content_type,
                    content_name: eventData.plan_name || 'Acesso VIP',
                    query: eventData.utm_term || ''
                },
                page: {
                    url: clickData?.landing_page || 'https://tracking.com',
                    referrer: clickData?.referrer || ''
                },
                // Adicionar parâmetros de contexto se disponíveis
                context: clickData?.ttclid ? {
                    ad: {
                        callback: clickData.ttclid
                    }
                } : undefined
            }
        ]
    };

    // Remover campos undefined
    const cleanPayload = JSON.parse(JSON.stringify(payload));

    // CORREÇÃO CRÍTICA: Só enviar test_event_code se for realmente teste
    if (isTest) {
        // Usar test_event_code do pixel ou padrão apenas em teste
        cleanPayload.test_event_code = pixel.test_event_code || 'TEST54815';
    }
    // NÃO enviar test_event_code em produção!

    try {
        console.log('📤 Enviando para TikTok:', JSON.stringify(cleanPayload, null, 2));

        const response = await axios.post(url, cleanPayload, {
            headers: {
                'Access-Token': pixel.access_token,
                'Content-Type': 'application/json'
            },
            timeout: 10000
        });

        console.log(`✅ TikTok: Evento Purchase enviado para ${eventData.sale_code}`);
        console.log('Resposta TikTok:', JSON.stringify(response.data, null, 2));

        return { success: true, data: response.data };

    } catch (error) {
        console.error('❌ TikTok Error:', {
            status: error.response?.status,
            data: error.response?.data,
            message: error.message,
            config: error.config?.data
        });
        return { success: false, error: error.message };
    }
}

// Enviar evento para Facebook - CORRIGIDA
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

    // CORREÇÃO: Valor em centavos para Facebook
    const valueInCents = eventData.plan_value * 100;

    const payload = {
        data: [{
            event_name: 'Purchase',
            event_time: Math.floor(Date.now() / 1000),
            event_id: eventData.sale_code,
            action_source: 'website',
            user_data: userData,
            custom_data: {
                value: valueInCents, // CORREÇÃO: Valor em centavos
                currency: eventData.currency || 'BRL'
            }
        }],
        access_token: pixel.access_token
    };

    // CORREÇÃO: Só enviar test_event_code se for teste
    if (isTest) {
        payload.test_event_code = pixel.test_event_code || 'TEST54815';
    }

    try {
        const response = await axios.post(url, payload);
        console.log(`✅ Facebook: Evento enviado para ${eventData.sale_code}`);
        return { success: true, data: response.data };
    } catch (error) {
        console.error('❌ Facebook Error:', error.response?.data || error.message);
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
async function processApexEvent(eventData) {
    console.log(`\n💰💰💰 PROCESSANDO ${eventData.event.toUpperCase()} 💰💰💰`);
    console.log('══════════════════════════════════════════════════════════════');
    console.log('📨 WEBHOOK DA APEX:');
    console.log(JSON.stringify(eventData, null, 2));

    // 1) Definir sale_code como IDENTIDADE ÚNICA DA VENDA
    const rawSaleCode =
        eventData.transaction?.sale_code ||
        eventData.transaction?.external_transaction_id ||
        null;

    const saleCode = rawSaleCode || `APEX_${eventData.timestamp}`;
    console.log('🧾 SALE_CODE:', saleCode);

    // 2) Tentar carregar venda já existente
    const existingSale = await getSaleBySaleCode(saleCode);
    if (existingSale) {
        console.log('🔁 Venda existente encontrada no banco:', existingSale.sale_code);
    } else {
        console.log('🆕 Nenhuma venda encontrada, será criada uma nova.');
    }

    // 3) Resolver CLICK_ID real (SEM inventar)
    let clickData = null;
    let clickId = existingSale?.click_id || null;

    // Lista de candidatos pra buscar click
    const clickCandidates = [];

    // tracking.utm_id (caso a Apex envie)
    if (eventData.tracking?.utm_id) {
        clickCandidates.push(eventData.tracking.utm_id);
    }

    // sale_code (muito importante pro seu caso — pixel.gif usa ele como click_id)
    if (rawSaleCode) {
        clickCandidates.push(rawSaleCode.toString());
    }

    // external_transaction_id, se tiver
    if (eventData.transaction?.external_transaction_id) {
        clickCandidates.push(eventData.transaction.external_transaction_id.toString());
    }

    // chat_id (em alguns setups vira parte do click_id/start)
    if (eventData.customer?.chat_id) {
        clickCandidates.push(eventData.customer.chat_id.toString());
    }

    // Se já tinha click_id salvo na venda anterior, testar ele também
    if (existingSale?.click_id) {
        clickCandidates.push(existingSale.click_id);
    }

    // Remover duplicados e falsy
    const uniqueCandidates = [...new Set(clickCandidates.filter(Boolean))];

    console.log('🎯 Candidatos a CLICK_ID para busca:', uniqueCandidates);

    // Buscar click real no banco
    for (const candidate of uniqueCandidates) {
        if (clickData) break;
        console.log(`🔍 Tentando buscar click com id/campo: "${candidate}"`);
        const found = await getClick(candidate);
        if (found) {
            clickData = found;
            clickId = found.click_id;
            console.log('✅ CLICK ENCONTRADO:', clickId);
            break;
        }
    }

    // Se ainda não encontrou, tenta busca avançada usando saleCode
    if (!clickData && saleCode) {
        console.log('⚠️ Click não encontrado nos candidatos. Tentando busca avançada com saleCode...');
        const advanced = await findClickByMultipleCriteria(saleCode.toString());
        if (advanced) {
            clickData = advanced;
            clickId = advanced.click_id;
            console.log('✅ CLICK ENCONTRADO (busca avançada):', clickId);
        }
    }

    if (!clickData) {
        console.log('⚠️ Nenhum CLICK REAL encontrado. Venda será salva SEM click_id associado.');
        clickId = null; // regra de ouro: NÃO inventar click_id
    }

    // 4) Determinar status interno da venda
    let status = 'pending';
    if (eventData.event === 'payment_created') {
        status = 'created';
    } else if (eventData.event === 'payment_approved') {
        status = 'approved';
    }

    // 5) Determinar UTMs com prioridade:
    //    1) dados do click
    //    2) dados da venda já salva
    //    3) tracking do webhook
    const utm_source =
        clickData?.utm_source ||
        existingSale?.utm_source ||
        eventData.tracking?.utm_source ||
        null;

    const utm_medium =
        clickData?.utm_medium ||
        existingSale?.utm_medium ||
        eventData.tracking?.utm_medium ||
        null;

    const utm_campaign =
        clickData?.utm_campaign ||
        existingSale?.utm_campaign ||
        eventData.tracking?.utm_campaign ||
        null;

    const utm_content =
        clickData?.utm_content ||
        existingSale?.utm_content ||
        eventData.tracking?.utm_content ||
        null;

    const utm_term =
        clickData?.utm_term ||
        existingSale?.utm_term ||
        eventData.tracking?.utm_term ||
        null;

    // 6) Montar saleData final (base pra salvar + UTMify)
    const saleData = {
        sale_code: saleCode,
        click_id: clickId,

        customer_name:
            eventData.customer?.full_name ||
            eventData.customer?.profile_name ||
            existingSale?.customer_name ||
            'Cliente Apex',

        customer_email:
            eventData.customer?.email ||
            existingSale?.customer_email ||
            (eventData.customer?.chat_id
                ? `user_${eventData.customer.chat_id}@apexvips.com`
                : 'naoinformado@apexvips.com'),

        customer_phone:
            (eventData.customer?.phone
                ? eventData.customer.phone.replace(/\D/g, '')
                : existingSale?.customer_phone) || null,

        customer_document:
            eventData.customer?.tax_id ||
            existingSale?.customer_document ||
            null,

        plan_name:
            eventData.transaction?.plan_name ||
            existingSale?.plan_name ||
            'Plano Apex',

        plan_value:
            eventData.transaction?.plan_value
                ? eventData.transaction.plan_value / 100
                : existingSale?.plan_value ||
                0,

        currency:
            eventData.transaction?.currency ||
            existingSale?.currency ||
            'BRL',

        payment_platform:
            eventData.transaction?.payment_platform ||
            existingSale?.payment_platform ||
            'apexvips',

        payment_method:
            eventData.transaction?.payment_method ||
            existingSale?.payment_method ||
            'unknown',

        ip: eventData.origin?.ip || existingSale?.ip || '0.0.0.0',
        user_agent:
            eventData.origin?.user_agent ||
            existingSale?.user_agent ||
            'ApexVips/1.0',

        utm_source,
        utm_medium,
        utm_campaign,
        utm_content: utm_content || '',
        utm_term: utm_term || '',

        created_at: existingSale?.created_at || eventData.timestamp,
        approved_at:
            status === 'approved'
                ? eventData.timestamp
                : existingSale?.approved_at || null,

        status
    };

    // 7) Salvar venda (create ou update – controlado pelo ON CONFLICT)
    const saveResult = await saveSale(saleData);

    console.log(`\n💾 VENDA SALVA/ATUALIZADA: ${saleData.sale_code} (${status})`);
    console.log('📊 TRACKING ASSOCIADO:');
    console.log('├─ Click ID:', saleData.click_id || 'NENHUM');
    console.log('├─ UTM Source:', saleData.utm_source || 'NULL');
    console.log('├─ UTM Campaign:', saleData.utm_campaign || 'NULL');
    console.log('└─ UTM Medium:', saleData.utm_medium || 'NULL');

    console.log('\n🎯 O QUE SERÁ ENVIADO PARA UTMIFY:');
    console.log('├─ Sale Code:', saleData.sale_code);
    console.log('├─ Original Sale Code da Apex:', rawSaleCode);
    console.log('├─ Click ID usado:', saleData.click_id || 'NENHUM');
    console.log('├─ Customer:', saleData.customer_name);
    console.log('├─ Value: R$', saleData.plan_value.toFixed(2));
    console.log('└─ UTM Parameters:');
    console.log('   ├─ source:', saleData.utm_source || 'direct');
    console.log('   ├─ medium:', saleData.utm_medium || 'organic');
    console.log('   ├─ campaign:', saleData.utm_campaign || 'default');
    console.log('   ├─ content:', saleData.utm_content || '');
    console.log('   └─ term:', saleData.utm_term || '');

    // 8) ENVIAR PARA UTMIFY (sempre que houver API KEY e evento de pagamento)
    if (UTMIFY_API_KEY && (eventData.event === 'payment_created' || eventData.event === 'payment_approved')) {
        console.log('\n🔄 ENVIANDO PARA UTMIFY...');
        const utmifyResult = await sendToUtmify(saleData, clickData);

        console.log('🔄 RESULTADO UTMIFY:', utmifyResult.success ? '✅ SUCESSO' : '❌ FALHA');
        if (!utmifyResult.success && utmifyResult.error) {
            console.log('📋 Erro detalhado:', utmifyResult.error);
        }

        // 9) Processar pixels apenas em approved
        if (eventData.event === 'payment_approved' && saveResult.success) {
            console.log('\n🎯 PROCESSANDO PIXELS...');
            const pixelResults = await processPixelEvents(saleData, clickData, false);
            const successCount = pixelResults.filter(p => p.success).length;
            console.log('✅ Pixels processados:', successCount, 'sucesso(s) de', pixelResults.length);

            if (successCount < pixelResults.length) {
                pixelResults.filter(p => !p.success).forEach(p => {
                    console.log(`   ❌ ${p.platform}: ${p.error}`);
                });
            }
        }
    } else if (!UTMIFY_API_KEY) {
        console.log('\n⚠️ UTMIFY_API_KEY não configurada - pulando envio');
    } else {
        console.log('\nℹ️ Evento não requer envio para UTMify:', eventData.event);
    }

    console.log('\n══════════════════════════════════════════════════════════════');
    console.log(`✅ ${eventData.event.toUpperCase()} PROCESSADO`);
    console.log('══════════════════════════════════════════════════════════════\n');
}

// Função para buscar click por múltiplos critérios
async function findClickByMultipleCriteria(clickId) {
    if (!clickId) return null;

    console.log(`🔍 Busca avançada por click: "${clickId}"`);

    try {
        // Tentar diferentes formas de busca
        const queries = [
            // 1. Busca exata por click_id
            { query: 'SELECT * FROM clicks WHERE click_id = $1', params: [clickId] },

            // 2. Busca por utm_id
            { query: 'SELECT * FROM clicks WHERE utm_id = $1', params: [clickId] },

            // 3. Busca por parte do click_id (para casos como "clk_1765079740645_ohfgh0")
            { query: 'SELECT * FROM clicks WHERE click_id LIKE $1', params: [`%${clickId}%`] },

            // 4. Busca por utm_content (pode conter click_id)
            { query: 'SELECT * FROM clicks WHERE utm_content LIKE $1', params: [`%${clickId}%`] },

            // 5. Busca por sale_code em vendas (se já houver venda associada)
            {
                query: `SELECT c.* FROM clicks c 
                     JOIN sales s ON c.click_id = s.click_id 
                     WHERE s.sale_code = $1`, params: [clickId]
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

// Rota 5: Redirecionamento com tracking
app.get('/redirect', async (req, res) => {
    try {
        const { click_id, url, ...params } = req.query;

        // URL de destino (Telegram por padrão)
        let destination = url || TELEGRAM_BOT_URL;

        // Se for Telegram, adicionar click_id como parâmetro start
        if ((destination.includes('t.me') || destination.includes('telegram.me')) && click_id) {
            const urlObj = new URL(destination);
            urlObj.searchParams.set('start', click_id);
            destination = urlObj.toString();
        }

        // Registrar o clique
        if (click_id) {
            const clickData = {
                click_id,
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

        // Redirecionar imediatamente
        res.redirect(302, destination);

    } catch (error) {
        console.error('❌ Erro no redirect:', error.message);
        res.redirect(302, TELEGRAM_BOT_URL);
    }
});

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

// Reenviar todas as vendas para UTMify (reprocessamento em massa)
app.post('/admin/resend-utmify', async (req, res) => {
    try {
        const { only_missing = false, limit = null } = req.body || {};

        if (!UTMIFY_API_KEY) {
            return res.status(400).json({
                success: false,
                error: 'UTMIFY_API_KEY não configurada. Configure antes de reenviar vendas.'
            });
        }

        // Montar query base
        let query = 'SELECT * FROM sales';
        const params = [];

        if (only_missing) {
            query += ' WHERE utmify_sent = FALSE';
        }

        query += ' ORDER BY created_at ASC';

        if (limit && Number.isInteger(limit)) {
            query += ` LIMIT ${limit}`;
        }

        const result = await pool.query(query, params);
        const sales = result.rows;

        console.log(`\n🔄 Reprocessando ${sales.length} venda(s) para UTMify (only_missing=${only_missing})`);

        const summary = {
            total: sales.length,
            success: 0,
            failed: 0,
            details: []
        };

        for (const sale of sales) {
            try {
                // Buscar click associado (se houver)
                let clickData = null;
                if (sale.click_id) {
                    const clickRes = await pool.query(
                        'SELECT * FROM clicks WHERE click_id = $1 LIMIT 1',
                        [sale.click_id]
                    );
                    if (clickRes.rows.length > 0) {
                        clickData = clickRes.rows[0];
                    }
                }

                // Montar saleData no formato esperado pelo sendToUtmify
                const saleData = {
                    sale_code: sale.sale_code,
                    click_id: sale.click_id,

                    customer_name: sale.customer_name || 'Cliente',
                    customer_email: sale.customer_email || 'naoinformado@utmify.com',
                    customer_phone: sale.customer_phone,
                    customer_document: sale.customer_document,

                    plan_name: sale.plan_name || 'Plano',
                    plan_value: parseFloat(sale.plan_value || 0),
                    currency: sale.currency || 'BRL',
                    payment_platform: sale.payment_platform || 'ApexVips',
                    payment_method: sale.payment_method || 'unknown',

                    ip: sale.ip || clickData?.ip || '0.0.0.0',
                    user_agent: sale.user_agent || 'Reprocess/1.0',

                    utm_source: sale.utm_source || clickData?.utm_source,
                    utm_medium: sale.utm_medium || clickData?.utm_medium,
                    utm_campaign: sale.utm_campaign || clickData?.utm_campaign,
                    utm_content: sale.utm_content || clickData?.utm_content || '',
                    utm_term: sale.utm_term || clickData?.utm_term || '',

                    status: sale.status || 'created',
                    approved_at: sale.approved_at
                        ? Math.floor(new Date(sale.approved_at).getTime() / 1000)
                        : null
                };

                const utmResult = await sendToUtmify(saleData, clickData);

                summary.details.push({
                    sale_code: sale.sale_code,
                    status: sale.status,
                    utmify_status: utmResult.success ? 'success' : 'failed',
                    error: utmResult.success ? null : utmResult.error || null
                });

                if (utmResult.success) {
                    summary.success += 1;
                } else {
                    summary.failed += 1;
                }

            } catch (innerError) {
                console.error('❌ Erro ao reenviar venda para UTMify:', innerError.message);
                summary.failed += 1;
                summary.details.push({
                    sale_code: sale.sale_code,
                    status: sale.status,
                    utmify_status: 'failed',
                    error: innerError.message
                });
            }
        }

        res.json({
            success: true,
            message: 'Reprocessamento de vendas finalizado',
            report: summary
        });

    } catch (error) {
        console.error('❌ Erro em /admin/resend-utmify:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
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

        // Construir query dinâmica
        const updates = [];
        const values = [];
        let paramCount = 1;

        if (name !== undefined) {
            updates.push(`name = $${paramCount}`);
            values.push(name);
            paramCount++;
        }

        if (platform !== undefined) {
            updates.push(`platform = $${paramCount}`);
            values.push(platform);
            paramCount++;
        }

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

        // Sempre atualizar updated_at
        updates.push(`updated_at = CURRENT_TIMESTAMP`);

        if (updates.length === 1) { // apenas updated_at
            return res.status(400).json({ error: 'Nenhum campo para atualizar' });
        }

        values.push(pixelId);
        const query = `
            UPDATE pixels 
            SET ${updates.join(', ')}
            WHERE id = $${paramCount}
            RETURNING *;
        `;

        const result = await pool.query(query, values);

        console.log(`✏️ Pixel atualizado: ${result.rows[0].name}`);

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

            // Top campanhas por conversão (CORRIGIDA)
            pool.query(`
                SELECT 
                    s.utm_source,
                    s.utm_campaign,
                    COUNT(DISTINCT s.sale_code) as sales_count,
                    SUM(s.plan_value) as total_revenue,
                    COUNT(DISTINCT c.click_id) as clicks_count,
                    CASE 
                        WHEN COUNT(DISTINCT c.click_id) > 0 
                        THEN ROUND((COUNT(DISTINCT s.sale_code) * 100.0 / COUNT(DISTINCT c.click_id)), 2)
                        ELSE 0
                    END as conversion_rate
                FROM sales s
                LEFT JOIN clicks c ON s.click_id = c.click_id
                WHERE s.utm_source IS NOT NULL 
                    AND s.created_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY s.utm_source, s.utm_campaign
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
                conversion_rate: parseFloat(row.conversion_rate)
            })),

            // Timestamp
            generated_at: new Date().toISOString(),
            period: 'last_30_days'
        });

    } catch (error) {
        console.error('❌ Erro ao buscar estatísticas:', error.message);
        res.status(500).json({ error: error.message });
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