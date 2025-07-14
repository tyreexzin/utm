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

app.use(cors({ 
    origin: '*', 
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE', 
    credentials: true, 
    optionsSuccessStatus: 204 
}));
app.use(express.json());

// --- Variáveis de Ambiente e Constantes ---
const TELEGRAM_SESSION = process.env.TELEGRAM_SESSION;
const DATABASE_URL = process.env.DATABASE_URL;
const PORT = process.env.PORT;
const API_KEY = process.env.API_KEY; 
const PUSHINPAY_API_TOKEN = process.env.PUSHINPAY_API_TOKEN;

// [ALTERAÇÃO 1]: Variáveis de ambiente para os dois Pixels do Facebook.
// Lembre-se de adicionar estas variáveis ao seu arquivo .env
// Pixel Principal
const FACEBOOK_PIXEL_ID_1 = process.env.FACEBOOK_PIXEL_ID_1;
const FACEBOOK_API_TOKEN_1 = process.env.FACEBOOK_API_TOKEN_1;
// Pixel Secundário (para aquecimento)
const FACEBOOK_PIXEL_ID_2 = process.env.FACEBOOK_PIXEL_ID_2;
const FACEBOOK_API_TOKEN_2 = process.env.FACEBOOK_API_TOKEN_2;


const apiId = 23313993; 
const apiHash = 'd9249aed345807c04562fb52448a878c'; 
const stringSession = new StringSession(TELEGRAM_SESSION || '');
const CHAT_ID = BigInt(-1002733614113);

// --- Configuração do Banco de Dados PostgreSQL ---
const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

pool.on('connect', () => {
    console.log('✅ PostgreSQL conectado!');
});

pool.on('error', (err) => {
    console.error('❌ Erro inesperado no pool do PostgreSQL:', err);
    process.exit(1);
});

// --- Função Auxiliar para Criptografia ---
function hashData(data) {
    if (!data) {
        return null;
    }
    const cleanedData = String(data).replace(/[^0-9]/g, '');
    const hash = crypto.createHash('sha256').update(cleanedData.toLowerCase().trim()).digest('hex');
    return hash;
}

// --- FUNÇÕES DO BANCO DE DADOS ---
async function setupDatabase() {
    console.log('🔄 Iniciando configuração do banco de dados...');
    const client = await pool.connect();
    try {
        const sqlVendas = `
            CREATE TABLE IF NOT EXISTS vendas (
                id SERIAL PRIMARY KEY, 
                chave TEXT UNIQUE NOT NULL, 
                hash TEXT UNIQUE NOT NULL, 
                valor REAL NOT NULL, 
                utm_source TEXT, 
                utm_medium TEXT, 
                utm_campaign TEXT, 
                utm_content TEXT, 
                utm_term TEXT, 
                order_id TEXT, 
                transaction_id TEXT, 
                ip TEXT, 
                user_agent TEXT, 
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
                facebook_purchase_sent BOOLEAN DEFAULT FALSE
            );
        `;
        await client.query(sqlVendas);
        console.log('✅ Tabela "vendas" verificada.');

        const sqlFrontendUtms = `
            CREATE TABLE IF NOT EXISTS frontend_utms (
                id SERIAL PRIMARY KEY, 
                unique_click_id TEXT UNIQUE NOT NULL, 
                timestamp_ms BIGINT NOT NULL, 
                valor REAL, 
                fbclid TEXT, 
                fbc TEXT, 
                fbp TEXT, 
                utm_source TEXT, 
                utm_medium TEXT, 
                utm_campaign TEXT, 
                utm_content TEXT, 
                utm_term TEXT, 
                ip TEXT, 
                user_agent TEXT, 
                received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `;
        await client.query(sqlFrontendUtms);
        console.log('✅ Tabela "frontend_utms" verificada.');

        const sqlTelegramUsers = `
            CREATE TABLE IF NOT EXISTS telegram_users (
                telegram_user_id TEXT PRIMARY KEY, 
                unique_click_id TEXT, 
                last_activity TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        `;
        await client.query(sqlTelegramUsers);
        console.log('✅ Tabela "telegram_users" verificada.');

    } catch (err) {
        console.error('❌ Erro ao configurar tabelas no PostgreSQL:', err.message);
        process.exit(1);
    } finally {
        client.release();
    }
}

function gerarChaveUnica({ transaction_id }) { 
    return `chave-${transaction_id}`; 
}

function gerarHash({ transaction_id }) { 
    return `hash-${transaction_id}`; 
}

async function salvarVenda(venda) {
    console.log('💾 Tentando salvar venda no banco (PostgreSQL)...');
    const sql = `
        INSERT INTO vendas (
            chave, hash, valor, utm_source, utm_medium, utm_campaign, utm_content, utm_term, 
            order_id, transaction_id, ip, user_agent, facebook_purchase_sent
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) 
        ON CONFLICT (hash) DO NOTHING;
    `;
    const valores = [
        venda.chave, 
        venda.hash, 
        venda.valor, 
        venda.utm_source, 
        venda.utm_medium, 
        venda.utm_campaign, 
        venda.utm_content, 
        venda.utm_term, 
        venda.orderId, 
        venda.transaction_id, 
        venda.ip, 
        venda.userAgent, 
        venda.facebook_purchase_sent
    ];
    try {
        const res = await pool.query(sql, valores);
        if (res.rowCount > 0) {
            console.log('✅ Venda salva no PostgreSQL!');
        } else {
            console.log('🔁 Venda já existia no PostgreSQL, ignorando inserção (hash duplicado).');
        }
    } catch (err) {
        console.error('❌ Erro ao salvar venda no DB (PostgreSQL):', err.message);
    }
}

async function vendaExiste(hash) {
    console.log(`🔎 Verificando se venda com hash ${hash} existe no PostgreSQL...`);
    const sql = 'SELECT COUNT(*) AS total FROM vendas WHERE hash = $1';
    try {
        const res = await pool.query(sql, [hash]);
        return res.rows[0].total > 0;
    } catch (err) {
        console.error('❌ Erro ao verificar venda existente (PostgreSQL):', err.message);
        return false;
    }
}

async function saveUserClickAssociation(telegramUserId, uniqueClickId) {
    const sql = `
        INSERT INTO telegram_users (telegram_user_id, unique_click_id, last_activity) 
        VALUES ($1, $2, NOW()) 
        ON CONFLICT (telegram_user_id) DO UPDATE SET 
            unique_click_id = EXCLUDED.unique_click_id, 
            last_activity = NOW();
    `;
    try {
        await pool.query(sql, [telegramUserId, uniqueClickId]);
        console.log(`✅ Associação user_id(${telegramUserId}) -> click_id(${uniqueClickId}) salva no DB.`);
    } catch (err) {
        console.error('❌ Erro ao salvar associação user_id-click_id no DB:', err.message);
    }
}

async function salvarFrontendUtms(data) {
    console.log('💾 Tentando salvar UTMs do frontend no banco (PostgreSQL)...');
    const sql = `
        INSERT INTO frontend_utms (
            unique_click_id, timestamp_ms, valor, fbclid, fbc, fbp, utm_source, utm_medium, 
            utm_campaign, utm_content, utm_term, ip, user_agent
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
    `;
    const valores = [
        data.unique_click_id, data.timestamp, data.valor, 
        data.fbclid || null, data.fbc || null, data.fbp || null, 
        data.utm_source || null, data.utm_medium || null, data.utm_campaign || null, 
        data.utm_content || null, data.utm_term || null, 
        data.ip || null, data.user_agent || null
    ];
    try {
        await pool.query(sql, valores);
        console.log('✅ UTMs do frontend salvas no PostgreSQL!');
    } catch (err) {
        console.error('❌ Erro ao salvar UTMs do frontend no DB (PostgreSQL):', err.message);
    }
}

async function buscarUtmsPorUniqueClickId(uniqueClickId) {
    console.log(`🔎 Buscando UTMs do frontend por unique_click_id: ${uniqueClickId}...`);
    const sql = 'SELECT * FROM frontend_utms WHERE unique_click_id = $1 ORDER BY received_at DESC LIMIT 1';
    try {
        const res = await pool.query(sql, [uniqueClickId]);
        if (res.rows.length > 0) {
            console.log(`✅ UTMs encontradas para unique_click_id ${uniqueClickId}.`);
            return res.rows[0];
        } else {
            console.log(`🔎 Nenhuma UTM do frontend encontrada para unique_click_id ${uniqueClickId}.`);
            return null;
        }
    } catch (err) {
        console.error('❌ Erro ao buscar UTMs por unique_click_id (PostgreSQL):', err.message);
        return null;
    }
}

async function limparFrontendUtmsAntigos() {
    console.log('🧹 Iniciando limpeza de UTMs antigos...');
    const cutoffTime = moment().subtract(24, 'hours').valueOf();
    const sql = `DELETE FROM frontend_utms WHERE timestamp_ms < $1`;
    try {
        const res = await pool.query(sql, [cutoffTime]);
        if (res.rowCount > 0) {
            console.log(`🧹 Limpeza de UTMs antigos: ${res.rowCount || 0} registros removidos.`);
        }
    } catch (err) {
        console.error('❌ Erro ao limpar UTMs antigos:', err.message);
    }
}

// [ALTERAÇÃO 2]: Função reutilizável para enviar eventos para a API de Conversões do Facebook.
/**
 * @description Envia um evento para a API de Conversões do Facebook.
 * @param {string} pixelId - O ID do Pixel do Facebook para o qual o evento será enviado.
 * @param {string} apiToken - O token de acesso da API de Conversões.
 * @param {object} payload - O corpo da requisição (payload) contendo os dados do evento.
 * @param {string} transaction_id - O ID da transação, usado para logs.
 * @returns {Promise<boolean>} Retorna true se o evento foi enviado com sucesso, caso contrário, false.
 */
async function enviarEventoParaFacebook(pixelId, apiToken, payload, transaction_id) {
    // Verifica se o ID do Pixel e o Token foram fornecidos.
    if (!pixelId || !apiToken) {
        console.log(`⚠️  [BOT] Pixel ID ou API Token não configurado. Envio para este pixel será ignorado.`);
        return false;
    }

    console.log(`➡️  [BOT] Enviando evento 'Purchase' (ID: ${transaction_id}) para o Pixel ${pixelId}...`);

    try {
        // Realiza a requisição para a API do Facebook.
        await axios.post(
            `https://graph.facebook.com/v19.0/${pixelId}/events?access_token=${apiToken}`,
            payload
        );
        console.log(`✅ [BOT] Evento enviado com sucesso para o Pixel ${pixelId}.`);
        return true;
    } catch (err) {
        // Em caso de erro, exibe uma mensagem detalhada.
        console.error(`❌ [BOT] Erro ao enviar para o Pixel ${pixelId}:`, err.response?.data?.error || err.message);
        return false;
    }
}


// --- ENDPOINTS HTTP ---
app.post('/frontend-utm-data', (req, res) => {
    console.log('🚀 [BACKEND] Dados do frontend recebidos:', req.body);
    if (!req.body.unique_click_id || !req.body.timestamp) {
        return res.status(400).send('unique_click_id e Timestamp são obrigatórios.');
    }
    salvarFrontendUtms(req.body);
    res.status(200).send('Dados recebidos com sucesso!');
});

app.get('/ping', (req, res) => {
    console.log('💚 [PING] Recebida requisição /ping. Serviço está ativo.');
    res.status(200).send('Pong!');
});

// --- INICIALIZAÇÃO E LÓGICA PRINCIPAL ---
app.listen(PORT || 3000, () => {
    console.log(`🌐 Servidor HTTP Express escutando na porta ${PORT || 3000}.`);

    // --- LÓGICA DE AUTO-PING ---
    // Define o intervalo do ping em minutos.
    const PING_INTERVALO_MINUTOS = 1; 
    const PING_INTERVALO_MS = PING_INTERVALO_MINUTOS * 60 * 1000;

    // A função que fará o ping para manter o serviço ativo.
    const selfPing = async () => {
        // O Render define esta variável de ambiente com a URL pública do seu serviço.
        const url = process.env.RENDER_EXTERNAL_URL; 
        
        if (url) {
            try {
                // Faz a requisição para a rota /ping da própria aplicação.
                await axios.get(`${url}/ping`); 
            } catch (err) {
                console.error('❌ Erro no auto-ping:', err.message);
            }
        }
    };
    
    // --- LÓGICA DE INICIALIZAÇÃO ASSÍNCRONA ---
    (async () => {
        await setupDatabase();
        
        setInterval(limparFrontendUtmsAntigos, 60 * 60 * 1000);
        console.log('🧹 Limpeza de UTMs antigos agendada para cada 1 hora.');

        // Inicia o intervalo do auto-ping.
        setInterval(selfPing, PING_INTERVALO_MS);
        console.log(`🔁 Auto-ping configurado para cada ${PING_INTERVALO_MINUTOS} minuto(s).`);

        if (!TELEGRAM_SESSION) {
            return console.error("❌ ERRO FATAL: TELEGRAM_SESSION não definida.");
        }

        console.log('Iniciando userbot...');
        const client = new TelegramClient(new StringSession(TELEGRAM_SESSION), parseInt(apiId), apiHash, { connectionRetries: 5 });
        
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

        // --- MANIPULAÇÃO DE MENSAGENS ---
        // CORREÇÃO: Adicionado 'async' para permitir o uso de 'await' dentro do handler.
        client.addEventHandler(async (event) => {
            const message = event.message;
            if (!message || message.chatId.toString() !== CHAT_ID.toString()) {
                return;
            }

            let texto = (message.message || '').replace(/\r/g, '').trim();
            if (texto.startsWith('/start ')) {
                const startPayload = decodeURIComponent(texto.substring('/start '.length).trim());
                await saveUserClickAssociation(message.senderId.toString(), startPayload);
                return;
            }

            const idMatch = texto.match(/ID\s+Transa(?:ç|c)[aã]o\s+Gateway[:：]?\s*([\w-]+)/i);
            const valorLiquidoMatch = texto.match(/Valor\s+L[ií]quido[:：]?\s*R?\$?\s*([\d.,]+)/i);
            
            if (!idMatch || !valorLiquidoMatch) {
                return;
            }

            try {
                const transaction_id = idMatch[1].trim();
                const hash = gerarHash({ transaction_id });

                if (await vendaExiste(hash)) {
                    console.log(`🔁 Venda com hash ${hash} já registrada. Ignorando.`);
                    return;
                }
                
                console.log(`\n⚡ Nova venda detectada! Processando ID: ${transaction_id}`);

                // --- 1. DADOS PRIMÁRIOS (DA MENSAGEM DO TELEGRAM) ---
                const nomeCompletoRegex = /Nome\s+Completo[:：]?\s*(.+)/i;
                const emailRegex = /E-mail[:：]?\s*(\S+@\S+\.\S+)/i;
                const codigoVendaRegex = /Código\s+de\s+Venda[:：]?\s*(.+)/i;
                const plataformaPagamentoRegex = /Plataforma\s+Pagamento[:：]?\s*(.+)/i;
                const metodoPagamentoRegex = /M[ée]todo\s+Pagamento[:：]?\s*(.+)/i;
                
                const nomeMatch = texto.match(nomeCompletoRegex);
                const emailMatch = texto.match(emailRegex);
                const codigoVendaMatch = texto.match(codigoVendaRegex);

                let nomeDaMensagem = "Cliente Desconhecido";
                if (nomeMatch && nomeMatch[1]) {
                    nomeDaMensagem = nomeMatch[1].trim().split('|')[0];
                }

                let emailDaMensagem = null;
                if (emailMatch && emailMatch[1]) {
                    emailDaMensagem = emailMatch[1].trim();
                }
                
                let valorDaMensagem = 0;
                if (valorLiquidoMatch && valorLiquidoMatch[1]) {
                    valorDaMensagem = parseFloat(valorLiquidoMatch[1].replace(/\./g, '').replace(',', '.'));
                }

                let codigoVendaDaMensagem = null;
                if (codigoVendaMatch && codigoVendaMatch[1]) {
                    codigoVendaDaMensagem = codigoVendaMatch[1].trim();
                }

                // --- 2. DADOS COMPLEMENTARES (DA API PUSHINPAY) ---
                let dadosDaApi = null;
                if (PUSHINPAY_API_TOKEN) {
                    console.log(`🔎 Consultando API da Pushinpay para a transação ${transaction_id}...`);
                    try {
                        const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction_id}`, {
                            headers: { 
                                'Authorization': `Bearer ${PUSHINPAY_API_TOKEN}`,
                                'Accept': 'application/json'
                            }
                        });
                        dadosDaApi = response.data;
                        console.log('✅ Dados da API Pushinpay obtidos com sucesso!');
                    } catch (apiError) {
                        console.warn(`⚠️  Não foi possível consultar a API da Pushinpay. Prosseguindo com os dados da mensagem.`);
                    }
                }

                // --- 3. COMBINAÇÃO DOS DADOS ---
                const finalCustomerName = dadosDaApi?.payer_name || nomeDaMensagem;
                const finalCustomerEmail = emailDaMensagem;
                const finalCustomerDocument = dadosDaApi?.payer_national_registration || null;
                const finalValor = valorDaMensagem;
                
                console.log(`   -> Valor Líquido: R$ ${finalValor.toFixed(2)} | Nome Final: ${finalCustomerName}`);
                if (finalCustomerDocument) {
                    console.log(`   -> ✅ Documento (CPF/CNPJ) do cliente obtido via API.`);
                } else {
                    console.log(`   -> ⚠️  Documento do cliente não encontrado.`);
                }

                let matchedFrontendUtms = null;
                if (codigoVendaDaMensagem) {
                    matchedFrontendUtms = await buscarUtmsPorUniqueClickId(codigoVendaDaMensagem);
                }
                if (matchedFrontendUtms) {
                    console.log(`✅ [BOT] UTMs para ${transaction_id} atribuídas!`);
                }

                let facebook_purchase_sent = false;

                // --- 4. ENVIO PARA UTMIFY ---
                if (API_KEY) {
                    let trackingParams = {
                        utm_source: null,
                        utm_medium: null,
                        utm_campaign: null,
                        utm_content: null,
                        utm_term: null,
                    };

                    if (matchedFrontendUtms) {
                        trackingParams.utm_source = matchedFrontendUtms.utm_source || null;
                        trackingParams.utm_medium = matchedFrontendUtms.utm_medium || null;
                        trackingParams.utm_campaign = matchedFrontendUtms.utm_campaign || null;
                        trackingParams.utm_content = matchedFrontendUtms.utm_content || null;
                        trackingParams.utm_term = matchedFrontendUtms.utm_term || null;
                    } else {
                        console.log(`⚠️ [BOT] Nenhuma UTM correspondente encontrada.`);
                    }

                    const platform = (texto.match(plataformaPagamentoRegex) || [])[1]?.trim() || 'UnknownPlatform';
                    const paymentMethod = (texto.match(metodoPagamentoRegex) || [])[1]?.trim().toLowerCase().replace(' ', '_') || 'unknown';
                    const agoraUtc = moment.utc().format('YYYY-MM-DD HH:mm:ss');
                    
                    const utmifyPayload = {
                        orderId: transaction_id,
                        platform: platform,
                        paymentMethod: paymentMethod,
                        status: 'paid',
                        createdAt: agoraUtc,
                        approvedDate: agoraUtc,
                        customer: {
                            name: finalCustomerName,
                            email: finalCustomerEmail || "naoinformado@utmify.com",
                            phone: null,
                            document: finalCustomerDocument,
                            ip: matchedFrontendUtms?.ip || '0.0.0.0'
                        },
                        products: [{
                            id: 'acesso-vip-bundle', name: 'Acesso VIP', planId: '', planName: '',
                            quantity: 1, priceInCents: Math.round(finalValor * 100)
                        }],
                        trackingParameters: trackingParams,
                        commission: {
                            totalPriceInCents: Math.round(finalValor * 100), gatewayFeeInCents: 0,
                            userCommissionInCents: Math.round(finalValor * 100), currency: 'BRL'
                        },
                        isTest: false
                    };
                    
                    try {
                        const res = await axios.post('https://api.utmify.com.br/api-credentials/orders', utmifyPayload, { headers: { 'x-api-token': API_KEY } });
                        console.log('📬 [BOT] Resposta da UTMify:', res.status, res.data);
                    } catch (err) { 
                        console.error('❌ [BOT] Erro ao enviar para UTMify:', err.response?.data || err.message); 
                    }
                }

                // [ALTERAÇÃO 3]: Bloco de envio para o Facebook modificado para suportar dois Pixels.
                // --- 5. ENVIO PARA FACEBOOK (MODIFICADO PARA DOIS PIXELS) ---
                console.log('➡️  [BOT] Iniciando envio para a API de Conversões do Facebook...');

                const nomeCompleto = finalCustomerName.toLowerCase().split(' ');
                const primeiroNome = nomeCompleto[0];
                const sobrenome = nomeCompleto.length > 1 ? nomeCompleto.slice(1).join(' ') : null;

                // --- Preparação dos Dados do Usuário (User Data) ---
                // Estes dados são comuns para ambos os pixels.
                const userData = {
                    fn: [hashData(primeiroNome)],
                    ln: [hashData(sobrenome)],
                    external_id: [hashData(finalCustomerDocument)],
                    client_ip_address: matchedFrontendUtms?.ip,
                    client_user_agent: matchedFrontendUtms?.user_agent,
                    fbc: matchedFrontendUtms?.fbc,
                    fbp: matchedFrontendUtms?.fbp,
                };

                // Remove campos que não têm valor para otimizar o envio.
                Object.keys(userData).forEach(key => {
                    if (!userData[key] || (Array.isArray(userData[key]) && !userData[key][0])) {
                        delete userData[key];
                    }
                });

                // --- Preparação do Payload do Evento ---
                // Este payload também é comum para ambos os pixels.
                const facebookPayload = {
                    data: [{
                        event_name: 'Purchase',
                        event_time: message.date,
                        event_id: transaction_id, // Usar o mesmo event_id ajuda o Facebook a desduplicar eventos se necessário.
                        action_source: 'website',
                        user_data: userData,
                        custom_data: {
                            value: finalValor,
                            currency: 'BRL'
                        }
                    }]
                };

                // --- Envio para os Pixels ---
                // Envia para o primeiro pixel e depois para o segundo.
                const envioPixel1 = await enviarEventoParaFacebook(FACEBOOK_PIXEL_ID_1, FACEBOOK_API_TOKEN_1, facebookPayload, transaction_id);
                const envioPixel2 = await enviarEventoParaFacebook(FACEBOOK_PIXEL_ID_2, FACEBOOK_API_TOKEN_2, facebookPayload, transaction_id);

                // Atualiza a flag se pelo menos um dos envios teve sucesso.
                // Isso é útil para o seu banco de dados saber que a compra foi registrada no Facebook.
                facebook_purchase_sent = envioPixel1 || envioPixel2;

                
                // --- 6. SALVAMENTO FINAL NO BANCO ---
                await salvarVenda({
                    chave: gerarChaveUnica({ transaction_id }),
                    hash: gerarHash({ transaction_id }),
                    valor: finalValor,
                    utm_source: matchedFrontendUtms?.utm_source,
                    utm_medium: matchedFrontendUtms?.utm_medium,
                    utm_campaign: matchedFrontendUtms?.utm_campaign,
                    utm_content: matchedFrontendUtms?.utm_content,
                    utm_term: matchedFrontendUtms?.utm_term,
                    orderId: transaction_id,
                    transaction_id: transaction_id,
                    ip: matchedFrontendUtms?.ip,
                    userAgent: matchedFrontendUtms?.user_agent,
                    facebook_purchase_sent: facebook_purchase_sent
                });

            } catch (err) {
                console.error('❌ [BOT] Erro geral ao processar mensagem:', err.message);
            }
        });
    })();
});