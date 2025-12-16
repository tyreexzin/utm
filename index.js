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
const FACEBOOK_PIXEL_ID = process.env.FACEBOOK_PIXEL_ID;
const FACEBOOK_API_TOKEN = process.env.FACEBOOK_API_TOKEN;
const PUSHINPAY_API_TOKEN = process.env.PUSHINPAY_API_TOKEN;

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
                kwai_click_id TEXT,
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
                kwai_click_id TEXT,  -- <-- ADICIONE "TEXT," AQUI
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

        const sqlKwaiEvents = `
        CREATE TABLE IF NOT EXISTS kwai_event_log (
            id SERIAL PRIMARY KEY,
            clickid TEXT NOT NULL,           -- kwai_click_id
            sale_code TEXT NOT NULL,         -- unique_click_id / sale_code
            event_name TEXT NOT NULL,        -- EVENT_PURCHASE, etc
            transaction_id TEXT,             -- ID da transação (opcional)
            sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(clickid, sale_code, event_name)  -- IMPEDE DUPLICAÇÃO
        );
        CREATE INDEX IF NOT EXISTS idx_kwai_event_composite ON kwai_event_log(clickid, sale_code, event_name);
    `;
        await client.query(sqlKwaiEvents);
        console.log('✅ Tabela "kwai_event_log" criada/verificada.');

    } catch (err) {
        console.error('❌ Erro ao configurar tabelas:', err.message);
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

// --- FUNÇÃO PARA REGISTRAR E VERIFICAR ENVIO KWAI ---
async function registrarEventoKwai(clickid, sale_code, event_name, transaction_id = null) {
    console.log(`🔎 Verificando duplicação para: ${event_name} | ClickID: ${clickid?.substring(0, 10)}... | SaleCode: ${sale_code?.substring(0, 8)}...`);

    if (!clickid || !sale_code || !event_name) {
        console.log('⚠️ Dados insuficientes para verificar duplicação.');
        return true; // Permite envio por segurança
    }

    const sql = `
        INSERT INTO kwai_event_log (clickid, sale_code, event_name, transaction_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (clickid, sale_code, event_name) DO NOTHING
        RETURNING id;
    `;

    try {
        const res = await pool.query(sql, [clickid, sale_code, event_name, transaction_id]);

        if (res.rows.length > 0) {
            console.log(`✅ Primeiro envio registrado para ${event_name}.`);
            return true; // É o primeiro envio, PODE ENVIAR
        } else {
            console.log(`⚠️ Evento ${event_name} JÁ FOI ENVIADO anteriormente para este clickid+sale_code.`);
            return false; // Já existe, NÃO ENVIAR (duplicação)
        }
    } catch (err) {
        console.error('❌ Erro ao verificar duplicação Kwai:', err.message);
        return true; // Em caso de erro, permite envio (fail-safe)
    }
}

async function salvarVenda(venda) {
    console.log('💾 Tentando salvar venda no banco (PostgreSQL)...');
    const sql = `
        INSERT INTO vendas (
            chave, hash, valor, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
            order_id, transaction_id, ip, user_agent, kwai_click_id, facebook_purchase_sent
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)  -- <-- MUDE $13 PARA $14
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
        venda.kwai_click_id,
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
            utm_campaign, utm_content, utm_term, ip, user_agent, kwai_click_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);  -- <-- MUDE $13 PARA $14
    `;
    const valores = [
        data.unique_click_id, data.timestamp, data.valor,
        data.fbclid || null, data.fbc || null, data.fbp || null,
        data.utm_source || null, data.utm_medium || null, data.utm_campaign || null,
        data.utm_content || null, data.utm_term || null,
        data.ip || null, data.user_agent || null, data.kwai_click_id || null
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

// --- FUNÇÃO PARA ENVIAR EVENTO PARA O KWAI ---
async function enviarEventoKwai({ eventoKwai, transactionData, kwaiClickId }) {
    // 1. Verificar credenciais essenciais
    const KWAIPIXEL_ID = process.env.KWAIPIXEL_ID;
    const KWAACCESS_TOKEN = process.env.KWAACCESS_TOKEN;

    if (!KWAIPIXEL_ID || !KWAACCESS_TOKEN) {
        console.error('❌ [KWAI] Credenciais não configuradas (pixelId ou access_token).');
        return; // Encerra a função silenciosamente se não há credenciais
    }

    if (!kwaiClickId) {
        console.log('⚠️ [KWAI] kwai_click_id não fornecido. Pulando envio.');
        return;
    }

    // 2. Determinar valor e moeda (principalmente para o evento PURCHASE)
    let valorEvento = 0;
    let currency = 'BRL';

    if (eventoKwai === 'EVENT_PURCHASE' && transactionData.plan_value) {
        // Supõe que o valor vem em centavos (ex: 4990) e converte para reais (49.90)
        valorEvento = (transactionData.plan_value / 100).toFixed(2);
        currency = transactionData.currency || 'BRL';
    }
    // Para EVENT_ADD_TO_CART, você pode querer enviar o valor do plano também.
    // Para EVENT_CONTENT_VIEW, o valor pode permanecer 0.

    // 3. Preparar o payload conforme a especificação do Kwai
    const kwaiPayload = {
        access_token: KWAACCESS_TOKEN,
        clickid: kwaiClickId,
        event_name: eventoKwai,
        pixelId: KWAIPIXEL_ID,
        testFlag: false,          // ALTERE PARA 'true' DURANTE TESTES NO PAINEL DO KWAI
        trackFlag: false,         // ALTERE PARA 'true' DURANTE TESTES NO PAINEL DO KWAI
        is_attributed: 1,
        mmpcode: 'PL',
        pixelSdkVersion: '9.9.9',
        currency: currency,
        value: valorEvento,
        // Propriedades do produto/evento (string JSON)
        properties: JSON.stringify({
            content_id: transactionData.sale_code || transactionData.internal_transaction_id || 'N/A',
            content_type: 'product',
            content_name: transactionData.plan_name || transactionData.category || 'Acesso VIP',
            quantity: 1,
            price: valorEvento
        })
    };

    // 4. Endpoint da API do Kwai
    const kwaiEndpoint = 'https://www.adsnebula.com/log/common/api';

    try {
        console.log(`➡️ [KWAI] Enviando evento '${eventoKwai}' para Click ID: ${kwaiClickId.substring(0, 15)}...`);

        const response = await axios.post(kwaiEndpoint, kwaiPayload, {
            headers: {
                'Content-Type': 'application/json',
                'accept': 'application/json;charset=utf-8'
            },
            timeout: 10000 // Timeout de 10 segundos para evitar que a requisição trave
        });

        // 5. Verificar a resposta da API do Kwai
        if (response.data && response.data.result === 1) {
            console.log(`✅ [KWAI] Evento '${eventoKwai}' enviado com sucesso.`);
        } else {
            // A API do Kwai retornou um erro de negócio (ex: parâmetro inválido)
            console.error(`❌ [KWAI] Erro na resposta da API:`, response.data);
        }
    } catch (error) {
        // 6. Tratamento de erros de rede ou da requisição
        console.error(`❌ [KWAI] Falha ao enviar o evento '${eventoKwai}':`, error.message);

        if (error.response) {
            // A requisição foi feita e o servidor respondeu com um status de erro (4xx, 5xx)
            console.error('❌ [KWAI] Detalhes do erro (status):', error.response.status);
            console.error('❌ [KWAI] Detalhes do erro (data):', error.response.data);
        } else if (error.request) {
            // A requisição foi feita mas nenhuma resposta foi recebida
            console.error('❌ [KWAI] Não foi possível obter resposta do servidor do Kwai.');
        }
        // O erro será propagado para que o endpoint do webhook possa fazer seu próprio tratamento
        throw error;
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

// --- ENDPOINTS HTTP ---
app.post('/frontend-utm-data', (req, res) => {
    console.log('🚀 [BACKEND] Dados do frontend recebidos:', req.body);
    if (!req.body.unique_click_id || !req.body.timestamp) {
        return res.status(400).send('unique_click_id e Timestamp são obrigatórios.');
    }
    salvarFrontendUtms(req.body);
    res.status(200).send('Dados recebidos com sucesso!');
});

// --- WEBHOOK APEXVIPS - Versão Simplificada ---
app.post('/webhook/apexvips', express.json(), async (req, res) => {
    console.log('📥 Webhook da ApexVips recebido:', req.body.event);

    const webhookData = req.body;

    // 1. Extrair o 'click_id' que é o nosso 'uniqueClickId' do frontend
    const frontendClickId = webhookData.tracking?.click_id;

    if (!frontendClickId) {
        console.log('⚠️ Webhook sem click_id (uniqueClickId). Ignorando.');
        return res.status(200).send('Evento sem click_id.'); // 200 para evitar retentativa
    }

    // 2. Buscar os UTMs (e o kwai_click_id) usando a função existente
    const matchedUtms = await buscarUtmsPorUniqueClickId(frontendClickId);

    if (!matchedUtms) {
        console.log(`⚠️ Dados de frontend não encontrados para click_id: ${frontendClickId}. O usuário pode não ter passado pela página de redirecionamento.`);
        return res.status(200).send('Dados de origem não encontrados.');
    }

    if (!matchedUtms.kwai_click_id) {
        console.log(`ℹ️  Este usuário (click_id: ${frontendClickId}) não veio de um clique no Kwai. Nenhum evento será enviado.`);
        return res.status(200).send('Não é tráfego do Kwai.');
    }

    // 3. Mapear o evento ApexVips para o evento Kwai
    const eventMapping = {
        'user_joined': 'EVENT_CONTENT_VIEW',
        'payment_created': 'EVENT_ADD_TO_CART',
        'payment_approved': 'EVENT_PURCHASE'
    };

    const kwaiEventName = eventMapping[webhookData.event];

    if (!kwaiEventName) {
        console.log(`⚠️ Evento '${webhookData.event}' não mapeado para Kwai.`);
        return res.status(200).send('Evento não mapeado.');
    }

    const podeEnviar = await registrarEventoKwai(
        matchedUtms.kwai_click_id,           // clickid
        frontendClickId,                     // sale_code (é o unique_click_id do frontend)
        kwaiEventName,                       // event_name (EVENT_PURCHASE, etc)
        webhookData.transaction?.internal_transaction_id // transaction_id
    );

    if (!podeEnviar) {
        console.log(`ℹ️  Evento ${kwaiEventName} JÁ ENVIADO anteriormente. Ignorando.`);
        return res.status(200).send('Evento já processado (duplicação evitada).');
    }

    // 5. SÓ ENVIA SE FOR O PRIMEIRO
    try {
        await enviarEventoKwai({
            eventoKwai: kwaiEventName,
            transactionData: webhookData.transaction || {},
            kwaiClickId: matchedUtms.kwai_click_id
        });
        console.log(`✅ [KWAI] Evento '${kwaiEventName}' enviado.`);
        res.status(200).send('Webhook processado com sucesso.');
    } catch (error) {
        console.error(`❌ [KWAI] Falha:`, error.message);
        res.status(200).send('Evento recebido.');
    }
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
                if (matchedFrontendUtms && matchedFrontendUtms.kwai_click_id) {
                    console.log(`✅ [BOT] UTMs para ${transaction_id} atribuídas!`);

                    // 1. VERIFICA DUPLICAÇÃO usando clickid + sale_code
                    const podeEnviar = await registrarEventoKwai(
                        matchedFrontendUtms.kwai_click_id,    // clickid
                        codigoVendaDaMensagem,               // sale_code (unique_click_id)
                        'EVENT_PURCHASE',                    // event_name
                        transaction_id                       // transaction_id (opcional)
                    );

                    // 2. SÓ ENVIA SE FOR O PRIMEIRO
                    if (podeEnviar) {
                        await enviarEventoKwai({
                            eventoKwai: 'EVENT_PURCHASE',
                            transactionData: {
                                sale_code: codigoVendaDaMensagem,
                                plan_value: finalValor * 100,
                                currency: 'BRL',
                                plan_name: 'Acesso VIP'
                            },
                            kwaiClickId: matchedFrontendUtms.kwai_click_id
                        });
                    } else {
                        console.log(`ℹ️  [BOT] EVENT_PURCHASE já enviado via outro fluxo.`);
                    }
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

                // --- 5. ENVIO PARA FACEBOOK ---
                if (FACEBOOK_PIXEL_ID && FACEBOOK_API_TOKEN) {
                    console.log('➡️  [BOT] Iniciando envio para API de Conversões do Facebook...');

                    const nomeCompleto = finalCustomerName.toLowerCase().split(' ');
                    const primeiroNome = nomeCompleto[0];
                    const sobrenome = nomeCompleto.length > 1 ? nomeCompleto.slice(1).join(' ') : null;

                    const userData = {
                        fn: [hashData(primeiroNome)],
                        ln: [hashData(sobrenome)],
                        external_id: [hashData(finalCustomerDocument)],
                        client_ip_address: matchedFrontendUtms?.ip,
                        client_user_agent: matchedFrontendUtms?.user_agent,
                        fbc: matchedFrontendUtms?.fbc,
                        fbp: matchedFrontendUtms?.fbp,
                    };

                    Object.keys(userData).forEach(key => {
                        if (!userData[key] || (Array.isArray(userData[key]) && userData[key].length === 0) || userData[key][0] === null) {
                            delete userData[key];
                        }
                    });

                    const facebookPayload = {
                        data: [{
                            event_name: 'Purchase',
                            event_time: message.date,
                            event_id: transaction_id,
                            action_source: 'website',
                            user_data: userData,
                            custom_data: {
                                value: finalValor,
                                currency: 'BRL'
                            }
                        }]
                    };

                    try {
                        await axios.post(`https://graph.facebook.com/v19.0/${FACEBOOK_PIXEL_ID}/events?access_token=${FACEBOOK_API_TOKEN}`, facebookPayload);
                        console.log(`✅ [BOT] Evento 'Purchase' (${transaction_id}) enviado para o Facebook.`);
                        facebook_purchase_sent = true;
                    } catch (err) {
                        console.error('❌ [BOT] Erro ao enviar para o Facebook:', err.response?.data?.error || err.message);
                    }
                }

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
                    kwaiClickId: matchedFrontendUtms?.kwai_click_id,
                    facebook_purchase_sent: facebook_purchase_sent
                });

            } catch (err) {
                console.error('❌ [BOT] Erro geral ao processar mensagem:', err.message);
            }
        });
    })();
});