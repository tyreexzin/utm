const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { NewMessage } = require('telegram/events');
const moment = require('moment');
const axios = require('axios');
const express = require('express');
const { Pool } = require('pg');
const crypto = require('crypto');
require('dotenv').config();
const cors = require('cors');

const app = express();

// --- Configuração do CORS e Express ---
const corsOptions = {
    origin: '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
    optionsSuccessStatus: 204
};
app.use(cors(corsOptions));
app.use(express.json());

// --- Variáveis de Ambiente e Constantes ---
const { 
    TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION, TELEGRAM_CHAT_ID,
    DATABASE_URL, PORT, UTMIFY_API_KEY, FACEBOOK_PIXEL_ID, FACEBOOK_API_TOKEN 
} = process.env;

const apiId = parseInt(TELEGRAM_API_ID);
const apiHash = TELEGRAM_API_HASH;
const stringSession = new StringSession(TELEGRAM_SESSION || '');
const CHAT_ID = BigInt(TELEGRAM_CHAT_ID);


// --- Configuração do Banco de Dados PostgreSQL ---
const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

pool.on('connect', () => console.log('✅ PostgreSQL conectado!'));
pool.on('error', (err) => {
    console.error('❌ Erro inesperado no pool do PostgreSQL:', err);
    process.exit(-1);
});

// --- Função Auxiliar para Criptografia SHA-256 ---
function hashData(data) {
    if (!data) return null;
    // Normaliza os dados (lowercase, trim) antes de criar o hash
    return crypto.createHash('sha256').update(String(data).toLowerCase().trim()).digest('hex');
}

// --- Funções do Banco de Dados ---
async function setupDatabase() {
    console.log('🔄 Iniciando configuração do banco de dados...');
    const client = await pool.connect();
    try {
        await client.query(`
            CREATE TABLE IF NOT EXISTS vendas (
                id SERIAL PRIMARY KEY,
                chave TEXT UNIQUE NOT NULL,
                hash TEXT UNIQUE NOT NULL,
                valor REAL NOT NULL,
                utm_source TEXT, utm_medium TEXT, utm_campaign TEXT, utm_content TEXT, utm_term TEXT,
                order_id TEXT, transaction_id TEXT,
                ip TEXT, user_agent TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                facebook_purchase_sent BOOLEAN DEFAULT FALSE,
                utmify_order_sent BOOLEAN DEFAULT FALSE
            );
        `);
        console.log(' -> Tabela "vendas" verificada/atualizada.');

        await client.query(`
            CREATE TABLE IF NOT EXISTS frontend_utms (
                id SERIAL PRIMARY KEY,
                unique_click_id TEXT UNIQUE NOT NULL, 
                timestamp_ms BIGINT NOT NULL,
                valor REAL, 
                fbclid TEXT, 
                fbc TEXT,
                fbp TEXT,
                utm_source TEXT, utm_medium TEXT, utm_campaign TEXT, utm_content TEXT, utm_term TEXT,
                ip TEXT, user_agent TEXT,
                received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log(' -> Tabela "frontend_utms" verificada/atualizada.');
        console.log('✅ Configuração do banco de dados concluída.');
    } finally {
        client.release();
    }
}

async function salvarVenda(venda) {
    console.log('💾 Tentando salvar o registro final da venda no banco de dados...');
    const sql = `
        INSERT INTO vendas (chave, hash, valor, utm_source, utm_medium, utm_campaign, utm_content, utm_term, order_id, transaction_id, ip, user_agent, facebook_purchase_sent, utmify_order_sent)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (hash) DO NOTHING;
    `;
    const valores = [
        `chave-${venda.transaction_id}`, `hash-${venda.transaction_id}`, venda.valor,
        venda.utm_source, venda.utm_medium, venda.utm_campaign, venda.utm_content, venda.utm_term,
        venda.orderId, venda.transaction_id, venda.ip, venda.userAgent,
        venda.facebook_purchase_sent, venda.utmify_order_sent
    ];
    try {
        const res = await pool.query(sql, valores);
        if (res.rowCount > 0) {
            console.log('✅ Registro final da venda salvo com sucesso!');
        }
    } catch (err) {
        console.error('❌ Erro ao salvar o registro final da venda no DB:', err.message);
    }
}

async function vendaExiste(hash) {
    console.log(`🔎 Verificando se a venda com hash ${hash} já existe...`);
    const res = await pool.query('SELECT 1 FROM vendas WHERE hash = $1', [hash]);
    return res.rowCount > 0;
}

async function salvarFrontendUtms(data) {
    const sql = `
        INSERT INTO frontend_utms (unique_click_id, timestamp_ms, valor, fbclid, fbc, fbp, utm_source, utm_medium, utm_campaign, utm_content, utm_term, ip, user_agent)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (unique_click_id) DO NOTHING;
    `;
    const valores = [
        data.unique_click_id, data.timestamp, data.valor, data.fbclid, data.fbc, data.fbp,
        data.utm_source, data.utm_medium, data.utm_campaign, data.utm_content, data.utm_term,
        data.ip, data.user_agent
    ];
    try {
        await pool.query(sql, valores);
    } catch(err) {
        console.error('❌ Erro ao salvar dados do clique no DB:', err.message);
    }
}

async function buscarUtmsPorUniqueClickId(uniqueClickId) {
    console.log(`🔎 Buscando dados do clique para o ID: ${uniqueClickId}...`);
    const res = await pool.query('SELECT * FROM frontend_utms WHERE unique_click_id = $1 LIMIT 1', [uniqueClickId]);
    return res.rows.length > 0 ? res.rows[0] : null;
}

async function limparFrontendUtmsAntigos() {
    console.log('🧹 Executando limpeza periódica de dados de cliques antigos...');
    try {
        const cutoffTime = moment().subtract(24, 'hours').valueOf();
        const res = await pool.query('DELETE FROM frontend_utms WHERE timestamp_ms < $1', [cutoffTime]);
        if (res.rowCount > 0) {
            console.log(`   -> Limpeza concluída: ${res.rowCount} registros antigos removidos.`);
        }
    } catch(err) {
        console.error('❌ Erro durante a limpeza de UTMs antigos:', err.message);
    }
}

// --- Endpoints HTTP ---
app.post('/frontend-utm-data', (req, res) => {
    console.log('🚀 [HTTP] Dados de clique recebidos do frontend.');
    salvarFrontendUtms(req.body);
    res.status(200).send('Dados recebidos com sucesso!');
});

app.get('/ping', (req, res) => {
    console.log('💚 [HTTP] Ping recebido. O serviço está ativo.');
    res.status(200).send('Pong!');
});

// --- LÓGICA PRINCIPAL ---
(async () => {
    await setupDatabase().catch(e => {
        console.error("❌ Falha crítica na configuração do banco de dados. O servidor não pode continuar.", e.message);
        process.exit(1);
    });

    app.listen(PORT || 3000, () => console.log(`🌐 Servidor Express escutando na porta ${PORT || 3000}.`));

    // Agenda a limpeza para rodar a cada hora
    setInterval(limparFrontendUtmsAntigos, 60 * 60 * 1000);

    if (!TELEGRAM_SESSION) {
        return console.error("❌ ERRO FATAL: A variável de ambiente TELEGRAM_SESSION não está definida. O bot não pode iniciar.");
    }
    
    console.log('▶️  Iniciando userbot do Telegram...');
    const client = new TelegramClient(stringSession, apiId, apiHash, { connectionRetries: 5 });

    await client.start({
        onError: (err) => console.log('❌ Erro durante o login do cliente Telegram:', err),
    });
    console.log('✅ Userbot do Telegram conectado e ouvindo mensagens!');

    // Manipulador de Novas Mensagens
    client.addEventHandler(async (event) => {
        const message = event.message;
        if (!message || message.chatId.toString() !== CHAT_ID.toString()) return;

        const texto = message.message || '';
        const idTransacaoMatch = texto.match(/ID Transa(?:ç|c)[aã]o\s+Gateway[:：]?\s*([\w-]+)/i);
        const valorLiquidoMatch = texto.match(/Valor\s+L[ií]quido[:：]?\s*R?\$?\s*([\d.,]+)/i);

        if (!idTransacaoMatch || !valorLiquidoMatch) {
            // Ignora mensagens que não são de venda
            return;
        }

        const transaction_id = idTransacaoMatch[1].trim();
        const hash = `hash-${transaction_id}`;

        if (await vendaExiste(hash)) {
            console.log(`🔁 Venda com ID ${transaction_id} já foi processada anteriormente. Ignorando.`);
            return;
        }

        console.log(`\n⚡ Nova venda detectada! Processando ID: ${transaction_id}`);
        
        // Extrai todos os dados possíveis da mensagem
        const valorLiquidoNum = parseFloat(valorLiquidoMatch[1].replace(/\./g, '').replace(',', '.').trim());
        const codigoVendaMatch = texto.match(/Código\s+de\s+Venda[:：]?\s*([\w-]+)/i);
        const nomeCompletoMatch = texto.match(/Nome\s+Completo[:：]?\s*(.+)/i);
        const emailMatch = texto.match(/E-mail[:：]?\s*(\S+@\S+\.\S+)/i);
        const plataformaMatch = texto.match(/Plataforma\s+Pagamento[:：]?\s*(.+)/i);
        const metodoMatch = texto.match(/M[ée]todo\s+Pagamento[:：]?\s*(.+)/i);
        
        const customerName = nomeCompletoMatch ? nomeCompletoMatch[1].trim().split('|')[0] : "Cliente";
        const customerEmail = emailMatch ? emailMatch[1].trim() : null;
        const codigoVenda = codigoVendaMatch ? codigoVendaMatch[1].trim() : null;
        
        const dadosDoClique = codigoVenda ? await buscarUtmsPorUniqueClickId(codigoVenda) : null;
        if (dadosDoClique) {
            console.log(`   -> Dados de clique correspondentes encontrados para o Código de Venda.`);
        } else {
            console.log(`   -> ⚠️ Nenhum dado de clique encontrado para o Código de Venda: ${codigoVenda}`);
        }
        
        let utmify_order_sent = false;
        let facebook_purchase_sent = false;

        // --- 1. Bloco de Envio para UTMify ---
        if (UTMIFY_API_KEY) {
            console.log('➡️  Iniciando envio para UTMify...');
            const utmifyPayload = {
                orderId: transaction_id,
                platform: plataformaMatch ? plataformaMatch[1].trim() : 'UnknownPlatform',
                paymentMethod: metodoMatch ? metodoMatch[1].trim().toLowerCase().replace(' ', '_') : 'unknown',
                status: 'paid',
                createdAt: moment.utc(message.date * 1000).format('YYYY-MM-DD HH:mm:ss'),
                approvedDate: moment.utc(message.date * 1000).format('YYYY-MM-DD HH:mm:ss'),
                customer: { name: customerName, email: customerEmail, ip: dadosDoClique?.ip || null },
                products: [{ id: 'acesso-vip', name: 'Acesso VIP', quantity: 1, priceInCents: Math.round(valorLiquidoNum * 100) }],
                trackingParameters: {
                    utm_source: dadosDoClique?.utm_source, utm_medium: dadosDoClique?.utm_medium,
                    utm_campaign: dadosDoClique?.utm_campaign, utm_content: dadosDoClique?.utm_content,
                    utm_term: dadosDoClique?.utm_term
                }
            };
            try {
                await axios.post('https://api.utmify.com.br/api-credentials/orders', utmifyPayload, {
                    headers: { 'x-api-token': UTMIFY_API_KEY, 'Content-Type': 'application/json' }
                });
                console.log(`   -> ✅ Sucesso: Pedido ${transaction_id} enviado para UTMify.`);
                utmify_order_sent = true;
            } catch (err) {
                console.error('   -> ❌ Erro ao enviar para UTMify:', err.response?.data || err.message);
            }
        } else {
            console.log('   -> ⚠️  Chave da UTMify não configurada. Pulando etapa.');
        }

        // --- 2. Bloco de Envio para Facebook ---
        if (FACEBOOK_PIXEL_ID && FACEBOOK_API_TOKEN) {
            console.log('➡️  Iniciando envio para API de Conversões do Facebook...');
            const nomeCompleto = customerName.toLowerCase().split(' ');
            const userData = {
                em: customerEmail ? [hashData(customerEmail)] : [],
                fn: [hashData(nomeCompleto[0])],
                ln: nomeCompleto.length > 1 ? [hashData(nomeCompleto.slice(1).join(' '))] : [],
                client_ip_address: dadosDoClique?.ip || null,
                client_user_agent: dadosDoClique?.user_agent || null,
                fbc: dadosDoClique?.fbc || null,
                fbp: dadosDoClique?.fbp || null,
            };
            // Limpa chaves nulas do objeto userData para garantir a validade do payload
            Object.keys(userData).forEach(key => (userData[key] === null || userData[key].length === 0 || !userData[key][0]) && delete userData[key]);

            const facebookPayload = {
                data: [{
                    event_name: 'Purchase',
                    event_time: message.date,
                    event_id: transaction_id,
                    action_source: 'website',
                    user_data: userData,
                    custom_data: { value: valorLiquidoNum, currency: 'BRL' }
                }]
            };

            try {
                await axios.post(`https://graph.facebook.com/v19.0/${FACEBOOK_PIXEL_ID}/events?access_token=${FACEBOOK_API_TOKEN}`, facebookPayload);
                console.log(`   -> ✅ Sucesso: Evento 'Purchase' (${transaction_id}) enviado para o Facebook.`);
                facebook_purchase_sent = true;
            } catch (err) {
                console.error('   -> ❌ Erro ao enviar para o Facebook:', err.response?.data?.error || err.message);
            }
        } else {
            console.log('   -> ⚠️  Credenciais do Facebook não configuradas. Pulando etapa.');
        }

        // --- 3. Salva o registro final da venda no banco de dados ---
        await salvarVenda({
            transaction_id: transaction_id,
            valor: valorLiquidoNum,
            orderId: transaction_id,
            utm_source: dadosDoClique?.utm_source,
            utm_medium: dadosDoClique?.utm_medium,
            utm_campaign: dadosDoClique?.utm_campaign,
            utm_content: dadosDoClique?.utm_content,
            utm_term: dadosDoClique?.utm_term,
            ip: dadosDoClique?.ip,
            userAgent: dadosDoClique?.user_agent,
            facebook_purchase_sent: facebook_purchase_sent,
            utmify_order_sent: utmify_order_sent
        });

    }, new NewMessage({ chats: [CHAT_ID] }));

})();