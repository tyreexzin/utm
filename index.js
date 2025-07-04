const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { NewMessage } = require('telegram/events');
const input = require('input');
const moment = require('moment');
const axios = require('axios');
const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();
const cors = require('cors');
const crypto = require('crypto');

const app = express();
app.use(cors({ origin: '*', methods: 'GET,HEAD,PUT,PATCH,POST,DELETE', credentials: true, optionsSuccessStatus: 204 }));
app.use(express.json());

// --- Variáveis de Ambiente e Constantes ---
const { 
    TELEGRAM_SESSION, DATABASE_URL, PORT, 
    API_KEY, // Para UTMify
    FACEBOOK_PIXEL_ID, FACEBOOK_API_TOKEN
} = process.env;

const apiId = 23313993; 
const apiHash = 'd9249aed345807c04562fb52448a878c'; 
const stringSession = new StringSession(TELEGRAM_SESSION || '');
const CHAT_ID = BigInt(-1002733614113);

// --- Configuração do Banco de Dados PostgreSQL ---
const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});
pool.on('connect', () => console.log('✅ PostgreSQL conectado!'));
pool.on('error', (err) => { console.error('❌ Erro inesperado no pool do PostgreSQL:', err); process.exit(-1); });

// --- Função Auxiliar para Criptografia ---
function hashData(data) {
    if (!data) return null;
    return crypto.createHash('sha256').update(String(data).toLowerCase().trim()).digest('hex');
}

// --- FUNÇÃO PARA INICIALIZAR TABELAS NO POSTGRESQL ---
async function setupDatabase() {
    const client = await pool.connect();
    try {
        await client.query(`
            CREATE TABLE IF NOT EXISTS vendas (
                id SERIAL PRIMARY KEY, chave TEXT UNIQUE NOT NULL, hash TEXT UNIQUE NOT NULL,
                valor REAL NOT NULL, utm_source TEXT, utm_medium TEXT, utm_campaign TEXT, utm_content TEXT, utm_term TEXT,
                order_id TEXT, transaction_id TEXT, ip TEXT, user_agent TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                facebook_purchase_sent BOOLEAN DEFAULT FALSE
            );
        `);
        console.log('✅ Tabela "vendas" verificada/criada no PostgreSQL.');

        await client.query(`
            CREATE TABLE IF NOT EXISTS frontend_utms (
                id SERIAL PRIMARY KEY, unique_click_id TEXT UNIQUE NOT NULL, timestamp_ms BIGINT NOT NULL,
                valor REAL, fbclid TEXT, fbc TEXT, fbp TEXT, utm_source TEXT, utm_medium TEXT,
                utm_campaign TEXT, utm_content TEXT, utm_term TEXT, ip TEXT, user_agent TEXT,
                received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log('✅ Tabela "frontend_utms" verificada/criada/atualizada no PostgreSQL.');

        await client.query(`
            CREATE TABLE IF NOT EXISTS telegram_users (
                telegram_user_id TEXT PRIMARY KEY, unique_click_id TEXT,
                last_activity TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log('✅ Tabela "telegram_users" verificada/criada no PostgreSQL.');
    } catch (err) {
        console.error('❌ Erro ao configurar tabelas no PostgreSQL:', err.message);
        process.exit(1);
    } finally {
        client.release();
    }
}

// --- FUNÇÕES DE UTILIDADE (Originais Mantidas) ---
function gerarChaveUnica({ transaction_id }) { return `chave-${transaction_id}`; }
function gerarHash({ transaction_id }) { return `hash-${transaction_id}`; }

async function salvarVenda(venda) {
    console.log('💾 Tentando salvar venda no banco (PostgreSQL)...');
    const sql = `
        INSERT INTO vendas (chave, hash, valor, utm_source, utm_medium, utm_campaign, utm_content, utm_term, order_id, transaction_id, ip, user_agent, facebook_purchase_sent)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (hash) DO NOTHING;
    `;
    const valores = [venda.chave, venda.hash, venda.valor, venda.utm_source, venda.utm_medium, venda.utm_campaign, venda.utm_content, venda.utm_term, venda.orderId, venda.transaction_id, venda.ip, venda.userAgent, venda.facebook_purchase_sent];
    try {
        const res = await pool.query(sql, valores);
        if (res.rowCount > 0) console.log('✅ Venda salva no PostgreSQL!');
        else console.log('🔁 Venda já existia no PostgreSQL, ignorando inserção (hash duplicado).');
    } catch (err) { console.error('❌ Erro ao salvar venda no DB (PostgreSQL):', err.message); }
}

async function vendaExiste(hash) {
    console.log(`🔎 Verificando se venda com hash ${hash} existe no PostgreSQL...`);
    const sql = 'SELECT COUNT(*) AS total FROM vendas WHERE hash = $1';
    try {
        const res = await pool.query(sql, [hash]);
        return res.rows[0].total > 0;
    } catch (err) { console.error('❌ Erro ao verificar venda existente (PostgreSQL):', err.message); return false; }
}

async function saveUserClickAssociation(telegramUserId, uniqueClickId) {
    try {
        await pool.query(`INSERT INTO telegram_users (telegram_user_id, unique_click_id, last_activity) VALUES ($1, $2, NOW()) ON CONFLICT (telegram_user_id) DO UPDATE SET unique_click_id = EXCLUDED.unique_click_id, last_activity = NOW();`, [telegramUserId, uniqueClickId]);
        console.log(`✅ Associação user_id(${telegramUserId}) -> click_id(${uniqueClickId}) salva no DB.`);
    } catch (err) { console.error('❌ Erro ao salvar associação user_id-click_id no DB:', err.message); }
}

async function salvarFrontendUtms(data) {
    console.log('💾 Tentando salvar UTMs do frontend no banco (PostgreSQL)...');
    const sql = `INSERT INTO frontend_utms (unique_click_id, timestamp_ms, valor, fbclid, fbc, fbp, utm_source, utm_medium, utm_campaign, utm_content, utm_term, ip, user_agent) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);`;
    const valores = [data.unique_click_id, data.timestamp, data.valor, data.fbclid || null, data.fbc || null, data.fbp || null, data.utm_source || null, data.utm_medium || null, data.utm_campaign || null, data.utm_content || null, data.utm_term || null, data.ip || null, data.user_agent || null];
    try {
        await pool.query(sql, valores);
        console.log('✅ UTMs do frontend salvas no PostgreSQL!');
    } catch (err) { console.error('❌ Erro ao salvar UTMs do frontend no DB (PostgreSQL):', err.message); }
}

async function buscarUtmsPorUniqueClickId(uniqueClickId) {
    console.log(`🔎 Buscando UTMs do frontend por unique_click_id: ${uniqueClickId}...`);
    const sql = 'SELECT * FROM frontend_utms WHERE unique_click_id = $1 ORDER BY received_at DESC LIMIT 1';
    try {
        const res = await pool.query(sql, [uniqueClickId]);
        if (res.rows.length > 0) { console.log(`✅ UTMs encontradas para unique_click_id ${uniqueClickId}.`); return res.rows[0]; }
        else { console.log(`🔎 Nenhuma UTM do frontend encontrada para unique_click_id ${uniqueClickId}.`); return null; }
    } catch (err) { console.error('❌ Erro ao buscar UTMs por unique_click_id (PostgreSQL):', err.message); return null; }
}

async function limparFrontendUtmsAntigos() {
    console.log('🧹 Iniciando limpeza de UTMs antigos do frontend...');
    const cutoffTime = moment().subtract(24, 'hours').valueOf();
    const sql = `DELETE FROM frontend_utms WHERE timestamp_ms < $1`;
    try {
        const res = await pool.query(sql, [cutoffTime]);
        console.log(`🧹 Limpeza de UTMs antigos do frontend: ${res.rowCount || 0} registros removidos.`);
    } catch (err) { console.error('❌ Erro ao limpar UTMs antigos do frontend:', err.message); }
}

// --- ENDPOINT HTTP ---
app.post('/frontend-utm-data', (req, res) => {
    const { unique_click_id, timestamp, valor, fbclid, fbc, fbp, utm_source, utm_medium, utm_campaign, utm_content, utm_term, ip, user_agent } = req.body;
    console.log('🚀 [BACKEND] Dados do frontend recebidos:', req.body);
    if (!unique_click_id || !timestamp) { return res.status(400).send('unique_click_id e Timestamp são obrigatórios.'); }
    salvarFrontendUtms({ unique_click_id, timestamp, valor, fbclid, fbc, fbp, utm_source, utm_medium, utm_campaign, utm_content, utm_term, ip, user_agent });
    res.status(200).send('Dados recebidos com sucesso!');
});
app.get('/ping', (req, res) => { console.log('💚 [PING] Recebida requisição /ping. Serviço está ativo.'); res.status(200).send('Pong!'); });

// --- INICIALIZAÇÃO E LÓGICA PRINCIPAL ---
app.listen(PORT || 3000, () => {
    console.log(`🌐 Servidor HTTP Express escutando na porta ${PORT || 3000}.`);
    (async () => {
        await setupDatabase();
        setInterval(limparFrontendUtmsAntigos, 60 * 60 * 1000);
        console.log('🧹 Limpeza de UTMs antigos agendada para cada 1 hora.');

        console.log('Iniciando userbot...');
        const client = new TelegramClient(new StringSession(TELEGRAM_SESSION), apiId, apiHash, { connectionRetries: 5 });
        try {
            await client.start({
                phoneNumber: async () => input.text('Digite seu número com DDI: '),
                password: async () => input.text('Senha 2FA (se tiver): '),
                phoneCode: async () => input.text('Código do Telegram: '),
                onError: (err) => console.log('Erro login:', err),
            });
            console.log('✅ Userbot conectado!');
            console.log('🔑 Nova StringSession:', client.session.save());
        } catch (error) {
            console.error('❌ Falha ao iniciar o userbot:', error.message);
            process.exit(1);
        }

        client.addEventHandler(async (event) => {
            const message = event.message;
            if (!message || message.chatId.toString() !== CHAT_ID.toString()) return;

            let texto = (message.message || '').replace(/\r/g, '').trim();
            if (texto.startsWith('/start ')) {
                const startPayload = decodeURIComponent(texto.substring('/start '.length).trim());
                await saveUserClickAssociation(message.senderId.toString(), startPayload);
                return;
            }

            const idMatch = texto.match(/ID\s+Transa(?:ç|c)[aã]o\s+Gateway[:：]?\s*([\w-]+)/i);
            const valorLiquidoMatch = texto.match(/Valor\s+L[ií]quido[:：]?\s*R?\$?\s*([\d.,]+)/i);
            if (!idMatch || !valorLiquidoMatch) return;

            try {
                const transaction_id = idMatch[1].trim();
                const hash = gerarHash({ transaction_id });
                if (await vendaExiste(hash)) {
                    console.log(`🔁 Venda com hash ${hash} já registrada. Ignorando duplicata.`);
                    return;
                }
                
                let facebook_purchase_sent = false;
                const valorLiquidoNum = parseFloat(valorLiquidoMatch[1].replace(/\./g, '').replace(',', '.').trim());
                
                const nomeCompletoRegex = /Nome\s+Completo[:：]?\s*(.+)/i;
                const emailRegex = /E-mail[:：]?\s*(\S+@\S+\.\S+)/i;
                const codigoDeVendaRegex = /Código\s+de\s+Venda[:：]?\s*(.+)/i;
                const plataformaPagamentoRegex = /Plataforma\s+Pagamento[:：]?\s*(.+)/i;
                const metodoPagamentoRegex = /M[ée]todo\s+Pagamento[:：]?\s*(.+)/i;

                const customerName = (texto.match(nomeCompletoRegex) || [])[1]?.trim() || "Cliente Desconhecido";
                const customerEmail = (texto.match(emailRegex) || [])[1]?.trim();
                const extractedCodigoDeVenda = (texto.match(codigoDeVendaRegex) || [])[1]?.trim();
                
                let matchedFrontendUtms = null;
                if (extractedCodigoDeVenda) {
                    console.log(`🤖 [BOT] Tentando encontrar UTMs pelo Código de Venda: ${extractedCodigoDeVenda}`);
                    matchedFrontendUtms = await buscarUtmsPorUniqueClickId(extractedCodigoDeVenda);
                } else {
                    console.log(`⚠️ [BOT] Código de Venda não encontrado na mensagem.`);
                }

                if (matchedFrontendUtms) console.log(`✅ [BOT] UTMs para ${transaction_id} atribuídas!`);
                
                // --- Bloco de envio para UTMify ---
                const platform = (texto.match(plataformaPagamentoRegex) || [])[1]?.trim() || 'UnknownPlatform';
                const paymentMethod = (texto.match(metodoPagamentoRegex) || [])[1]?.trim().toLowerCase().replace(' ', '_') || 'unknown';
                const agoraUtc = moment.utc().format('YYYY-MM-DD HH:mm:ss');
                
                const utmifyPayload = {
                    orderId: transaction_id, platform, paymentMethod, status: 'paid', createdAt: agoraUtc, approvedDate: agoraUtc,
                    customer: { name: customerName, email: customerEmail || "naoinformado@utmify.com", phone: null, document: null, ip: matchedFrontendUtms?.ip || '0.0.0.0' },
                    products: [{ id: 'acesso-vip-bundle', name: 'Acesso VIP', planId: '', planName: '', quantity: 1, priceInCents: Math.round(valorLiquidoNum * 100) }],
                    trackingParameters: { utm_source: matchedFrontendUtms?.utm_source, utm_medium: matchedFrontendUtms?.utm_medium, utm_campaign: matchedFrontendUtms?.utm_campaign, utm_content: matchedFrontendUtms?.utm_content, utm_term: matchedFrontendUtms?.utm_term },
                    commission: { totalPriceInCents: Math.round(valorLiquidoNum * 100), gatewayFeeInCents: 0, userCommissionInCents: Math.round(valorLiquidoNum * 100), currency: 'BRL' },
                    isTest: false
                };
                
                try {
                    const res = await axios.post('https://api.utmify.com.br/api-credentials/orders', utmifyPayload, { headers: { 'x-api-token': API_KEY } });
                    console.log('📬 [BOT] Resposta da UTMify:', res.status, res.data);
                } catch (err) { console.error('❌ [BOT] Erro ao enviar para UTMify:', err.response?.data || err.message); }

                // --- Bloco de envio para Facebook ---
                if (FACEBOOK_PIXEL_ID && FACEBOOK_API_TOKEN) {
                    console.log('➡️  [BOT] Iniciando envio para API de Conversões do Facebook...');
                    const nomeCompleto = customerName.toLowerCase().split(' ');
                    const userData = {
                        fn: [hashData(nomeCompleto[0])],
                        ln: nomeCompleto.length > 1 ? [hashData(nomeCompleto.slice(1).join(' '))] : [],
                        client_ip_address: matchedFrontendUtms?.ip,
                        client_user_agent: matchedFrontendUtms?.user_agent,
                        fbc: matchedFrontendUtms?.fbc,
                        fbp: matchedFrontendUtms?.fbp,
                    };
                    Object.keys(userData).forEach(key => !userData[key] && delete userData[key]);
                    
                    const facebookPayload = { data: [{ event_name: 'Purchase', event_time: message.date, event_id: transaction_id, action_source: 'website', user_data: userData, custom_data: { value: valorLiquidoNum, currency: 'BRL' } }] };

                    try {
                        await axios.post(`https://graph.facebook.com/v19.0/${FACEBOOK_PIXEL_ID}/events?access_token=${FACEBOOK_API_TOKEN}`, facebookPayload);
                        console.log(`✅ [BOT] Evento 'Purchase' (${transaction_id}) enviado para o Facebook.`);
                        facebook_purchase_sent = true;
                    } catch (err) { console.error('❌ [BOT] Erro ao enviar para o Facebook:', err.response?.data?.error || err.message); }
                } else { console.log('⚠️  [BOT] Credenciais do Facebook não configuradas. Pulando etapa.'); }
                
                await salvarVenda({
                    chave: gerarChaveUnica({ transaction_id }), hash, valor: valorLiquidoNum,
                    utm_source: matchedFrontendUtms?.utm_source, utm_medium: matchedFrontendUtms?.utm_medium, utm_campaign: matchedFrontendUtms?.utm_campaign, utm_content: matchedFrontendUtms?.utm_content, utm_term: matchedFrontendUtms?.utm_term,
                    orderId: transaction_id, transaction_id, ip: matchedFrontendUtms?.ip, userAgent: matchedFrontendUtms?.user_agent,
                    facebook_purchase_sent
                });
            } catch (err) { console.error('❌ [BOT] Erro geral ao processar mensagem:', err.message); }
        }, new NewMessage({ chats: [CHAT_ID] }));
    })();
});