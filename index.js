// index.js - Backend Principal com UTMify (Sintaxe SQL Corrigida)
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
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
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

// 2. Buscar clique
async function getClick(clickId) {
    const query = 'SELECT * FROM clicks WHERE click_id = $1 LIMIT 1';
    try {
        const result = await pool.query(query, [clickId]);
        return result.rows[0] || null;
    } catch (error) {
        console.error('❌ Erro ao buscar clique:', error.message);
        return null;
    }
}

// 3. Salvar venda
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
            customer_phone = COALESCE(EXCLUDED.customer_phone, sales.customer_phone)
        RETURNING id;
    `;

    const values = [
        data.sale_code,
        data.click_id,
        data.customer_name,
        data.customer_email,
        data.customer_phone,
        data.customer_document,
        data.plan_name,
        data.plan_value,
        data.currency || 'BRL',
        data.payment_platform,
        data.payment_method,
        data.status || 'approved',
        data.ip,
        data.user_agent,
        data.utm_source,
        data.utm_medium,
        data.utm_campaign,
        data.utm_content || '',
        data.utm_term || '',
        data.approved_at ? new Date(data.approved_at * 1000) : new Date()
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

// --- FUNÇÃO UTMIFY ---
async function sendToUtmify(saleData, clickData) {
    if (!UTMIFY_API_KEY) {
        console.log('⚠️ UTMIFY_API_KEY não configurada');
        return { success: false, error: 'API key não configurada' };
    }

    try {
        const now = new Date();
        const payload = {
            orderId: saleData.sale_code,
            platform: saleData.payment_platform || 'unknown',
            paymentMethod: saleData.payment_method || 'unknown',
            status: 'paid',
            createdAt: now.toISOString().replace('T', ' ').substring(0, 19),
            approvedDate: now.toISOString().replace('T', ' ').substring(0, 19),
            customer: {
                name: saleData.customer_name || 'Cliente',
                email: saleData.customer_email || "naoinformado@utmify.com",
                phone: saleData.customer_phone || null,
                document: saleData.customer_document || null,
                ip: saleData.ip || clickData?.ip || '0.0.0.0'
            },
            products: [{
                id: 'acesso-vip',
                name: saleData.plan_name || 'Acesso VIP',
                quantity: 1,
                priceInCents: Math.round((saleData.plan_value || 0) * 100)
            }],
            trackingParameters: {
                utm_source: saleData.utm_source || clickData?.utm_source,
                utm_medium: saleData.utm_medium || clickData?.utm_medium,
                utm_campaign: saleData.utm_campaign || clickData?.utm_campaign,
                utm_content: saleData.utm_content || clickData?.utm_content,
                utm_term: saleData.utm_term || clickData?.utm_term
            },
            commission: {
                totalPriceInCents: Math.round((saleData.plan_value || 0) * 100),
                gatewayFeeInCents: 0,
                userCommissionInCents: Math.round((saleData.plan_value || 0) * 100),
                currency: saleData.currency || 'BRL'
            },
            isTest: false
        };

        const response = await axios.post(
            'https://api.utmify.com.br/api-credentials/orders',
            payload,
            {
                headers: {
                    'x-api-token': UTMIFY_API_KEY,
                    'Content-Type': 'application/json'
                }
            }
        );

        console.log(`✅ UTMify: Venda ${saleData.sale_code} enviada`);

        // Marcar como enviado no banco
        await pool.query(
            'UPDATE sales SET utmify_sent = TRUE WHERE sale_code = $1',
            [saleData.sale_code]
        );

        return { success: true, data: response.data };

    } catch (error) {
        console.error('❌ Erro ao enviar para UTMify:', {
            status: error.response?.status,
            data: error.response?.data,
            message: error.message
        });
        return { success: false, error: error.message };
    }
}

// --- FUNÇÕES DE PIXEL ---

// Enviar evento para TikTok
async function sendTikTokEvent(pixel, eventData, clickData) {
    const url = 'https://business-api.tiktok.com/open_api/v1.3/pixel/track/';

    // Preparar dados do usuário (hashed)
    const user = {};
    if (eventData.customer_email) {
        user.email = hashData(eventData.customer_email);
    }
    if (eventData.customer_phone) {
        user.phone = hashData(eventData.customer_phone.replace(/\D/g, ''));
    }

    // Construir payload do TikTok
    const payload = {
        pixel_code: pixel.pixel_id,
        event: 'CompletePayment',
        event_id: eventData.sale_code,
        timestamp: Math.floor(Date.now() / 1000).toString(),
        context: {
            ad: clickData?.ttclid ? { callback: clickData.ttclid } : undefined,
            page: {
                url: clickData?.landing_page || 'https://tracking.com'
            },
            user: {
                ip: eventData.ip || clickData?.ip || '',
                user_agent: eventData.user_agent || clickData?.user_agent || ''
            }
        },
        properties: {
            value: eventData.plan_value || 0,
            currency: eventData.currency || 'BRL',
            contents: [{
                content_id: 'vip_access',
                content_name: eventData.plan_name || 'Acesso VIP',
                price: eventData.plan_value || 0,
                quantity: 1
            }]
        }
    };

    // Adicionar dados do usuário se existirem
    if (Object.keys(user).length > 0) {
        payload.properties.user = user;
    }

    // Adicionar test event code se existir
    if (pixel.test_event_code) {
        payload.test_event_code = pixel.test_event_code;
    }

    try {
        const response = await axios.post(url, payload, {
            headers: {
                'Access-Token': pixel.access_token,
                'Content-Type': 'application/json'
            }
        });

        console.log(`✅ TikTok: Evento enviado para ${eventData.sale_code}`);
        return { success: true, data: response.data };

    } catch (error) {
        console.error('❌ TikTok Error:', {
            status: error.response?.status,
            data: error.response?.data,
            message: error.message
        });
        return { success: false, error: error.message };
    }
}

// Enviar evento para Facebook
async function sendFacebookEvent(pixel, eventData, clickData) {
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

    const payload = {
        data: [{
            event_name: 'Purchase',
            event_time: Math.floor(Date.now() / 1000),
            event_id: eventData.sale_code,
            action_source: 'website',
            user_data: userData,
            custom_data: {
                value: eventData.plan_value || 0,
                currency: eventData.currency || 'BRL'
            }
        }],
        access_token: pixel.access_token
    };

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
async function processPixelEvents(saleData, clickData) {
    const pixels = await getActivePixels();

    const results = [];
    for (const pixel of pixels) {
        try {
            let result;

            if (pixel.platform === 'tiktok') {
                result = await sendTikTokEvent(pixel, saleData, clickData);
            } else if (pixel.platform === 'facebook') {
                result = await sendFacebookEvent(pixel, saleData, clickData);
            }

            // Atualizar status na venda
            if (result?.success) {
                const column = pixel.platform === 'tiktok' ? 'tiktok_sent' : 'facebook_sent';
                await pool.query(
                    `UPDATE sales SET ${column} = TRUE WHERE sale_code = $1`,
                    [saleData.sale_code]
                );
            }

            results.push({
                platform: pixel.platform,
                success: result?.success || false,
                error: result?.error
            });

        } catch (error) {
            console.error(`❌ Erro ao processar pixel ${pixel.platform}:`, error.message);
            results.push({ platform: pixel.platform, success: false, error: error.message });
        }
    }

    return results;
}

// --- ROTAS ---

// Rota 0: Validação do webhook para Apex Vips (GET)
app.get('/api/webhook/apex', (req, res) => {
    console.log('✅ Validação do webhook (GET) recebida da Apex');
    res.json({
        status: 'webhook_ready',
        message: 'Webhook configurado corretamente',
        method: 'POST',
        endpoint: '/api/webhook/apex',
        expected_format: {
            event: 'payment_approved',
            transaction: { sale_code: '...', plan_value: 9700 },
            customer: { email: '...', phone: '...' },
            tracking: { utm_source: '...', utm_campaign: '...' }
        },
        timestamp: new Date().toISOString()
    });
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

// Rota 4: Webhook da Apex Vips
app.post('/api/webhook/apex', async (req, res) => {
    console.log('📨 Webhook recebido da Apex Vips');

    try {
        const eventData = req.body;

        // Validar dados básicos
        if (!eventData.event || !eventData.transaction?.sale_code) {
            return res.status(400).json({ error: 'Dados inválidos' });
        }

        // Processar apenas vendas aprovadas
        if (eventData.event === 'payment_approved') {
            console.log(`💰 Venda aprovada: ${eventData.transaction.sale_code}`);

            // Buscar click_id associado
            let clickId = eventData.tracking?.utm_id || eventData.transaction?.sale_code;
            let clickData = null;

            if (clickId) {
                clickData = await getClick(clickId);
            }

            // Preparar dados da venda
            const saleData = {
                sale_code: eventData.transaction.sale_code,
                click_id: clickData?.click_id,
                customer_name: eventData.customer?.full_name || eventData.customer?.profile_name,
                customer_email: eventData.customer?.email,
                customer_phone: eventData.customer?.phone,
                customer_document: eventData.customer?.tax_id,
                plan_name: eventData.transaction?.plan_name || 'Acesso VIP',
                plan_value: eventData.transaction?.plan_value ? (eventData.transaction.plan_value / 100) : 0,
                currency: eventData.transaction?.currency || 'BRL',
                payment_platform: eventData.transaction?.payment_platform,
                payment_method: eventData.transaction?.payment_method,
                ip: eventData.origin?.ip,
                user_agent: eventData.origin?.user_agent,
                utm_source: eventData.tracking?.utm_source,
                utm_medium: eventData.tracking?.utm_medium,
                utm_campaign: eventData.tracking?.utm_campaign,
                utm_content: eventData.tracking?.utm_content,
                utm_term: eventData.tracking?.utm_term,
                approved_at: eventData.timestamp
            };

            // 1. Salvar venda no banco
            const saveResult = await saveSale(saleData);

            // 2. Enviar eventos para pixels (Facebook, TikTok)
            const pixelResults = await processPixelEvents(saleData, clickData);

            // 3. Enviar para UTMify (se API key configurada)
            let utmifyResult = null;
            if (UTMIFY_API_KEY) {
                utmifyResult = await sendToUtmify(saleData, clickData);
            }

            res.json({
                success: true,
                message: 'Venda processada',
                sale_code: saleData.sale_code,
                saved: saveResult.success,
                pixels: pixelResults,
                utmify: utmifyResult
            });

        } else {
            // Para outros eventos, apenas confirmar recebimento
            res.json({ success: true, message: 'Evento recebido' });
        }

    } catch (error) {
        console.error('❌ Erro no webhook:', error.message);
        res.status(500).json({ error: 'Erro interno' });
    }
});

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

// Adicionar/atualizar pixel
app.post('/admin/pixels', async (req, res) => {
    try {
        const { name, platform, pixel_id, access_token, test_event_code } = req.body;

        if (!name || !platform || !pixel_id || !access_token) {
            return res.status(400).json({ error: 'Dados incompletos' });
        }

        const query = `
            INSERT INTO pixels (name, platform, pixel_id, access_token, test_event_code)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (platform, pixel_id) DO UPDATE SET
                name = EXCLUDED.name,
                access_token = EXCLUDED.access_token,
                test_event_code = EXCLUDED.test_event_code,
                is_active = TRUE
            RETURNING *;
        `;

        const result = await pool.query(query, [
            name, platform, pixel_id, access_token, test_event_code || null
        ]);

        res.json({ success: true, pixel: result.rows[0] });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Estatísticas
app.get('/admin/stats', async (req, res) => {
    try {
        const [clicks, sales, revenue, pixels] = await Promise.all([
            pool.query('SELECT COUNT(*) as count FROM clicks'),
            pool.query('SELECT COUNT(*) as count FROM sales WHERE status = $1', ['approved']),
            pool.query('SELECT SUM(plan_value) as total FROM sales WHERE status = $1', ['approved']),
            pool.query('SELECT COUNT(*) as count FROM pixels WHERE is_active = TRUE')
        ]);

        res.json({
            clicks: parseInt(clicks.rows[0].count),
            sales: parseInt(sales.rows[0].count),
            revenue: parseFloat(revenue.rows[0].total || 0).toFixed(2),
            active_pixels: parseInt(pixels.rows[0].count),
            utmify_configured: !!UTMIFY_API_KEY
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
📊 Endpoints:
   GET  /                   - Health check
   GET  /test               - Teste rápido
   POST /api/track          - Receber cliques
   GET  /pixel.gif          - Pixel tracking
   POST /api/webhook/apex   - Webhook Apex Vips
   GET  /redirect           - Redirecionamento
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