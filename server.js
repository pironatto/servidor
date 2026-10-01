"use strict";

const WebSocket = require("ws");
const mysql = require("mysql2/promise");
const crypto = require("crypto");
const rankingService = require("./rankingService");

const PORT = Number(process.env.PORT || 3000);

const TEMPO_PREPARACAO_MS = 2000;
const TEMPO_PERGUNTA_MS = 10000;
const TEMPO_FEEDBACK_MS = 2000;
const TEMPO_ESPERA_SINGLE_MS = 10000;
const TOTAL_PERGUNTAS = 5;

const db = mysql.createPool({
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "fantomas",
  database: process.env.DB_NAME || "quiz",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

const wss = new WebSocket.Server({
  port: PORT
});

/* ============================================================
 * Estado em memória
 * ============================================================ */

const userMateria = new Map();
const paresAtivos = new Map();
const partidasPorUsuario = new Map();
const timersSingle = new Map();
const usuariosConectados = new Map();

/* ============================================================
 * Helpers genéricos
 * ============================================================ */

function obterUsuarioConectado(ws) {
  return usuariosConectados.get(ws) || null;
}

function esperar(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function enviarJson(ws, dados) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(dados));
  }
}

function enviarParaJogadores(partida, dados) {
  if (!partida) return;

  const payload =
    typeof dados === "string" ? dados : JSON.stringify(dados);

  partida.parceiros.forEach((ws) => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  });
}

function limparTimer(partida) {
  if (partida && partida.timer) {
    clearTimeout(partida.timer);
    partida.timer = null;
  }
}

function limparTimerSingle(ws) {
  const timer = timersSingle.get(ws);

  if (timer) {
    clearTimeout(timer);
  }

  timersSingle.delete(ws);
}

function normalizarResposta(resposta) {
  if (typeof resposta !== "string") return "";
  return resposta.trim().toUpperCase();
}

/* ============================================================
 * Identificação do usuário
 * ============================================================ */

async function identificarUsuario(ws, data) {
  const usuarioId =
    typeof data.usuarioId === "string"
      ? data.usuarioId.trim()
      : "";


  if (!usuarioId) {
    enviarJson(ws, {
      tipo: "erro",
      mensagem: "ID do usuário não informado."
    });
    return false;
  }

  try {
    const [rows] = await db.execute(
      `SELECT id, nome, avatar_url
         FROM usuarios
        WHERE id = ?
        LIMIT 1`,
      [usuarioId]
    );

    if (!rows || rows.length === 0) {
      enviarJson(ws, {
        tipo: "erro",
        mensagem: "Usuário não encontrado no banco."
      });
      return false;
    }

    const usuario = rows[0];

    usuariosConectados.set(ws, {
      id: String(usuario.id),
      nome: String(usuario.nome)
    });

    await db.execute(
      `UPDATE usuarios SET ultimo_acesso = NOW() WHERE id = ?`,
      [usuario.id]
    );

    enviarJson(ws, {
      tipo: "usuarioIdentificado",
      usuarioId: String(usuario.id),
      nome: String(usuario.nome),
      avatarUrl: usuario.avatar_url || null   // 🆕
    });

    console.log(
      `Usuário identificado: ${usuario.nome} (${usuario.id})`
    );

    return true;
  } catch (erro) {
    console.error("Erro ao identificar usuário:", erro);

    enviarJson(ws, {
      tipo: "erro",
      mensagem: "Não foi possível identificar o usuário."
    });

    return false;
  }
}

/* ============================================================
 * Criação de partida
 * ============================================================ */

function criarPartida(jogadores, materia) {
  const partidaId = crypto.randomUUID();

  console.log(
    `\n🟢 [criarPartida] id=${partidaId}, materia=${materia}, jogadores=${jogadores.length}`
  );

  jogadores.forEach((ws, i) => {
    const u = obterUsuarioConectado(ws);
    console.log(
      `   jogador ${i}: ${u ? u.id + " (" + u.nome + ")" : "❌ NULL ❌"}`
    );
  });

  const partida = {
    parceiros: jogadores,
    materia,
    partidaIndividual: jogadores.length === 1,
    perguntasUsadas: new Set(),
    perguntaAtual: null,
    numeroPergunta: 0,
    respostasRecebidas: new Map(),
    aguardandoInicio: new Set(jogadores),
    pontuacoes: new Map(),
    estatisticas: new Map(),
    persistenciaInicial: null,
    timer: null,
    processandoRespostas: false,
    encerrando: false,

    motivo: "normal",
    usuariosQueSairam: new Set(),
    historicoRespostas: [],

    // Guardamos os IDs dos usuários por ws no momento da criação
    // para não depender de usuariosConectados depois (que pode
    // ser limpo pelo evento "close").
    idsPorWs: new Map(),
 // 🆕 Guarda o resultado do ELO por ws, pra enviar no "fim"
    resultadosRating: new Map(),
      // 🆕 Guarda o nome dos usuários por ws (igual ao idsPorWs)
  nomesPorWs: new Map()
  };

  jogadores.forEach((ws) => {
    limparTimerSingle(ws);
    userMateria.delete(ws);
    partidasPorUsuario.set(ws, partidaId);

    
    const usuario = obterUsuarioConectado(ws);
    partida.idsPorWs.set(ws, usuario ? usuario.id : null);
    partida.nomesPorWs.set(ws, usuario ? usuario.nome : null);   // 🆕


    partida.pontuacoes.set(ws, 0);
    partida.estatisticas.set(ws, {
      respostasTotais: 0,
      respostasCorretas: 0,
      sequenciaAtual: 0,
      melhorSequencia: 0
    });
  });

  paresAtivos.set(partidaId, partida);

  partida.persistenciaInicial = registrarPartidaNoBanco(
    partidaId,
    partida
  );

  partida.persistenciaInicial.catch((erro) => {
    console.error("Erro ao registrar partida no banco:", erro);
  });

  console.log(
    `Partida criada: ${partidaId} | ` +
    `matéria: ${materia} | ` +
    `jogadores: ${jogadores.length} | ` +
    `individual: ${partida.partidaIndividual}`
  );

  return partidaId;
}

async function registrarPartidaNoBanco(partidaId, partida) {
  await db.execute(
    `INSERT INTO partidas (
        id, materia, modo, status, total_perguntas, iniciada_em
     ) VALUES (?, ?, ?, 'em_andamento', ?, NOW())`,
    [
      partidaId,
      partida.materia,
      partida.partidaIndividual ? "individual" : "multiplayer",
      TOTAL_PERGUNTAS
    ]
  );

  for (const ws of partida.parceiros) {
    await registrarJogadorNaPartida(partidaId, ws, partida);
  }

  console.log(`Partida registrada no banco: ${partidaId}`);
}

async function registrarJogadorNaPartida(partidaId, ws, partida) {
  const usuarioId = partida.idsPorWs.get(ws);

  console.log(
    `   [registrarJogador] partida=${partidaId}, usuario=${usuarioId || "❌ NULL"}`
  );

  if (!usuarioId) {
    throw new Error("Jogador não identificado.");
  }

  try {
    await db.execute(
      `INSERT INTO partida_jogadores (
          partida_id, usuario_id, pontuacao, resultado,
          respostas_totais, respostas_corretas, melhor_sequencia,
          rating_anterior, rating_novo, variacao_rating, entrou_em
       ) VALUES (
          ?, ?, 0, NULL, 0, 0, 0, NULL, NULL, 0, NOW()
       )
       ON DUPLICATE KEY UPDATE entrou_em = entrou_em`,
      [partidaId, usuarioId]
    );
    console.log(`   ✅ partida_jogadores inserido: ${usuarioId}`);
  } catch (err) {
    console.error(
      `   ❌ FALHA ao inserir partida_jogadores: ${err.message}`
    );
    console.error(`      SQL code: ${err.code}, sqlState: ${err.sqlState}`);
    throw err;
  }
}

/* ============================================================
 * Cálculo de pontos durante a partida
 * ============================================================ */

function calcularPontos(partida) {
  if (!partida || !partida.perguntaAtual) return 0;

  const tempoRestante =
    partida.perguntaAtual.terminaEm - Date.now();

  return Math.max(0, tempoRestante / 1000);
}

/* ============================================================
 * Processamento de resposta
 * ============================================================ */

function processarResposta(
  ws,
  partida,
  partidaId,
  resposta,
  expirada = false
) {
  if (!partida || !partida.perguntaAtual) return false;
  if (partida.respostasRecebidas.has(ws)) return false;

  const respostaNormalizada = normalizarResposta(resposta);
  const respostaCorreta = partida.perguntaAtual.correta;

  const acertou =
    !expirada &&
    respostaNormalizada !== "" &&
    respostaNormalizada === respostaCorreta;

  const pontos = acertou ? calcularPontos(partida) : 0;

  const pontuacaoAnterior =
    Number(partida.pontuacoes.get(ws)) || 0;

  const pontuacaoAtual = pontuacaoAnterior + pontos;

  const estatisticas = partida.estatisticas.get(ws);

  estatisticas.respostasTotais += 1;

  if (acertou) {
    estatisticas.respostasCorretas += 1;
    estatisticas.sequenciaAtual += 1;

    estatisticas.melhorSequencia = Math.max(
      estatisticas.melhorSequencia,
      estatisticas.sequenciaAtual
    );
  } else {
    estatisticas.sequenciaAtual = 0;
  }

  partida.respostasRecebidas.set(ws, respostaNormalizada);
  partida.pontuacoes.set(ws, pontuacaoAtual);

  const usuarioId = partida.idsPorWs.get(ws) || null;

  partida.historicoRespostas.push({
    usuarioId,
    perguntaId: partida.perguntaAtual.id,
    resposta: respostaNormalizada,
    respostaCorreta,
    acertou,
    pontos,
    tempoRespostaMs: expirada
      ? TEMPO_PERGUNTA_MS
      : Math.min(
          TEMPO_PERGUNTA_MS,
          Math.max(
            0,
            Date.now() -
              (partida.perguntaAtual.inicio - TEMPO_PREPARACAO_MS)
          )
        )
  });

  enviarJson(ws, {
    tipo: "resultadoResposta",
    partidaId,
    resposta: respostaNormalizada,
    correta: respostaCorreta,
    acertou,
    pontos,
    pontuacaoTotal: pontuacaoAtual,
    expirada,
    tempoRestante: expirada
      ? 0
      : Math.max(0, partida.perguntaAtual.terminaEm - Date.now())
  });

  const adversario = partida.parceiros.find(
    (outro) => outro !== ws
  );

  if (adversario) {
    enviarJson(adversario, {
      tipo: "pontuacaoOponente",
      partidaId,
      pontos: pontuacaoAtual
    });
  }

  console.log(
    `Resposta validada | ` +
    `jogador: ${usuarioId || "desconhecido"} | ` +
    `resposta: ${respostaNormalizada || "SEM RESPOSTA"} | ` +
    `acertou: ${acertou} | ` +
    `expirada: ${expirada} | ` +
    `pontos: ${pontos.toFixed(1)}`
  );

  return true;
}

/* ============================================================
 * Timer e envio de perguntas
 * ============================================================ */

function iniciarTimerDaPergunta(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || !partida.perguntaAtual) return;

  limparTimer(partida);

  const tempoAteExpirar = Math.max(
    0,
    partida.perguntaAtual.terminaEm - Date.now()
  );

  partida.timer = setTimeout(async () => {
    const atual = paresAtivos.get(partidaId);

    if (
      !atual ||
      atual.encerrando ||
      atual.processandoRespostas ||
      !atual.perguntaAtual
    ) {
      return;
    }

    console.log(
      `Tempo esgotado: pergunta ${atual.numeroPergunta} | ` +
      `partida ${partidaId}`
    );

    const jogadoresSemResposta = atual.parceiros.filter(
      (ws) => !atual.respostasRecebidas.has(ws)
    );

    jogadoresSemResposta.forEach((ws) => {
      processarResposta(ws, atual, partidaId, "", true);
    });

    await verificarRespostasDaPartida(partidaId);
  }, tempoAteExpirar);
}

async function enviarPergunta(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || partida.encerrando) return;

  if (partida.numeroPergunta >= TOTAL_PERGUNTAS) {
    await finalizarPartida(partidaId);
    return;
  }

  if (
    partida.perguntaAtual !== null ||
    partida.processandoRespostas
  ) {
    console.warn(`Partida ocupada: ${partidaId}`);
    return;
  }

  try {
    const idsUsados = Array.from(partida.perguntasUsadas);

    let rows;

    if (idsUsados.length === 0) {
      [rows] = await db.query(
        `SELECT id, materia, pergunta, r1, r2, r3, r4, correta
           FROM questoes
          WHERE materia = ?
          ORDER BY RAND()
          LIMIT 1`,
        [partida.materia]
      );
    } else {
      // Placeholders dinâmicos para IN (...)
      const placeholders = idsUsados.map(() => "?").join(",");

      [rows] = await db.query(
        `SELECT id, materia, pergunta, r1, r2, r3, r4, correta
           FROM questoes
          WHERE materia = ?
            AND id NOT IN (${placeholders})
          ORDER BY RAND()
          LIMIT 1`,
        [partida.materia, ...idsUsados]
      );
    }

    if (!rows || rows.length === 0) {
      await finalizarPartida(partidaId);
      return;
    }

    const row = rows[0];

    const agoraServidor = Date.now();
    const inicioServidor = agoraServidor + TEMPO_PREPARACAO_MS;
    const terminaEmServidor = inicioServidor + TEMPO_PERGUNTA_MS;

    partida.perguntasUsadas.add(row.id);
    partida.numeroPergunta += 1;
    partida.respostasRecebidas.clear();

    partida.perguntaAtual = {
      id: row.id,
      correta: String(row.correta).trim().toUpperCase(),
      inicio: inicioServidor,
      terminaEm: terminaEmServidor
    };

    enviarParaJogadores(partida, {
      tipo: "itens",
      partidaId,
      itens: [
        String(row.id),
        row.materia,
        row.pergunta,
        row.r1,
        row.r2,
        row.r3,
        row.r4
      ],
      tempoTotal: TEMPO_PERGUNTA_MS / 1000,
      inicioServidor,
      terminaEmServidor,
      servidorAgora: agoraServidor
    });

    console.log(
      `Enviando pergunta ${partida.numeroPergunta}/${TOTAL_PERGUNTAS}: ` +
      `${partidaId} | ` +
      `preparação: ${TEMPO_PREPARACAO_MS}ms | ` +
      `resposta: ${TEMPO_PERGUNTA_MS}ms | ` +
      `termina em: ${terminaEmServidor}`
    );

    iniciarTimerDaPergunta(partidaId);
  } catch (erro) {
    console.error("Erro ao consultar perguntas:", erro);
    await finalizarPartida(partidaId);
  }
}

/* ============================================================
 * Resumo de respostas
 * ============================================================ */

function enviarResumoDasRespostas(partidaId, partida) {
  if (
    !partida ||
    partida.partidaIndividual ||
    partida.parceiros.length < 2 ||
    !partida.perguntaAtual
  ) {
    return;
  }

  const jogadorA = partida.parceiros[0];
  const jogadorB = partida.parceiros[1];

  const respostaA =
    partida.respostasRecebidas.get(jogadorA) || "";
  const respostaB =
    partida.respostasRecebidas.get(jogadorB) || "";

  const respostaCorreta = partida.perguntaAtual.correta;

  const acertouA =
    respostaA !== "" && respostaA === respostaCorreta;
  const acertouB =
    respostaB !== "" && respostaB === respostaCorreta;

  enviarJson(jogadorA, {
    tipo: "resumoRespostas",
    partidaId,
    respostaJogador: respostaA,
    respostaOponente: respostaB,
    acertouJogador: acertouA,
    acertouOponente: acertouB
  });

  enviarJson(jogadorB, {
    tipo: "resumoRespostas",
    partidaId,
    respostaJogador: respostaB,
    respostaOponente: respostaA,
    acertouJogador: acertouB,
    acertouOponente: acertouA
  });
}

async function verificarRespostasDaPartida(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (
    !partida ||
    partida.encerrando ||
    partida.processandoRespostas
  ) {
    return;
  }

  if (
    partida.respostasRecebidas.size <
    partida.parceiros.length
  ) {
    return;
  }

  partida.processandoRespostas = true;

  enviarResumoDasRespostas(partidaId, partida);

  partida.perguntaAtual = null;
  limparTimer(partida);

  console.log(
    `Todos responderam: ${partidaId}. ` +
    `Aguardando ${TEMPO_FEEDBACK_MS}ms.`
  );

  await esperar(TEMPO_FEEDBACK_MS);

  const atual = paresAtivos.get(partidaId);

  if (!atual || atual.encerrando) return;

  atual.respostasRecebidas.clear();
  atual.processandoRespostas = false;

  await enviarPergunta(partidaId);
}

/* ============================================================
 * Registro de início e resposta
 * ============================================================ */

function registrarInicioDaPartida(ws, partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || !partida.parceiros.includes(ws)) {
    return;
  }

  if (!partida.aguardandoInicio.has(ws)) {
    return;
  }

  partida.aguardandoInicio.delete(ws);

  console.log(
    `Jogadores prontos: ` +
    `${partida.parceiros.length - partida.aguardandoInicio.size}/` +
    `${partida.parceiros.length}`
  );

  if (partida.aguardandoInicio.size === 0) {
    void enviarPergunta(partidaId);
  }
}

function registrarResposta(ws, data) {
  const partida = paresAtivos.get(data.partidaId);

  if (
    !partida ||
    partida.encerrando ||
    !partida.parceiros.includes(ws) ||
    !partida.perguntaAtual ||
    partida.processandoRespostas
  ) {
    return;
  }

  if (partida.respostasRecebidas.has(ws)) {
    console.log("Resposta duplicada ignorada.");
    return;
  }

  const agora = Date.now();

  if (agora < partida.perguntaAtual.inicio) {
    console.log(
      `Resposta recebida durante a preparação e ignorada: ` +
      `${data.partidaId}`
    );
    return;
  }

  const expirou = agora >= partida.perguntaAtual.terminaEm;

  processarResposta(
    ws,
    partida,
    data.partidaId,
    expirou ? "" : data.resposta,
    expirou
  );

  void verificarRespostasDaPartida(data.partidaId);
}

/* ============================================================
 * Cálculo do resultado da partida
 * ============================================================ */

function calcularResultadoDaPartida(partida, ws) {
  const estatisticas = partida.estatisticas.get(ws);
  const pontuacao =
    Number(partida.pontuacoes.get(ws)) || 0;

  if (partida.partidaIndividual) {
    return {
      resultado: "individual",
      pontuacao,
      respostasTotais: estatisticas.respostasTotais,
      respostasCorretas: estatisticas.respostasCorretas,
      sequenciaAtual: estatisticas.sequenciaAtual,
      melhorSequencia: estatisticas.melhorSequencia
    };
  }

  const pontuacoes = partida.parceiros.map(
    (jogador) => Number(partida.pontuacoes.get(jogador)) || 0
  );

  const maiorPontuacao = Math.max(...pontuacoes);

  const quantidadeComMaiorPontuacao = pontuacoes.filter(
    (valor) => valor === maiorPontuacao
  ).length;

  let resultado;

  if (quantidadeComMaiorPontuacao > 1) {
    resultado = "empate";
  } else if (pontuacao === maiorPontuacao) {
    resultado = "vitoria";
  } else {
    resultado = "derrota";
  }

  return {
    resultado,
    pontuacao,
    respostasTotais: estatisticas.respostasTotais,
    respostasCorretas: estatisticas.respostasCorretas,
    sequenciaAtual: estatisticas.sequenciaAtual,
    melhorSequencia: estatisticas.melhorSequencia
  };
}

/* ============================================================
 * Persistência final
 * ============================================================ */

async function finalizarPartidaNoBanco(partidaId, partida) {
  const connection = await db.getConnection();

  console.log(
    `\n🟠 [finalizarPartidaNoBanco] INICIO: ${partidaId}, motivo=${partida.motivo}`
  );
  console.log(`   parceiros count: ${partida.parceiros.length}`);
  console.log(
    `   usuariosQueSairam count: ${partida.usuariosQueSairam.size}`
  );

  for (const ws of partida.parceiros) {
    const id = partida.idsPorWs.get(ws);
    console.log(`   - ws → usuarioId: ${id || "❌ NÃO MAPEADO"}`);
  }

  try {
    await connection.beginTransaction();

    await partida.persistenciaInicial;

    const statusFinal =
      partida.motivo === "ambosDesconectaram"
        ? "cancelada"
        : "finalizada";

    let vencedorId = null;

    if (
      !partida.partidaIndividual &&
      partida.motivo === "normal" &&
      partida.parceiros.length === 2
    ) {
      const wsA = partida.parceiros[0];
      const wsB = partida.parceiros[1];

      const idA = partida.idsPorWs.get(wsA);
      const idB = partida.idsPorWs.get(wsB);

      if (idA && idB) {
        const ptsA = Number(partida.pontuacoes.get(wsA)) || 0;
        const ptsB = Number(partida.pontuacoes.get(wsB)) || 0;

        if (ptsA > ptsB) vencedorId = idA;
        else if (ptsB > ptsA) vencedorId = idB;
      }
    }

    const ratings = new Map();

    if (
      !partida.partidaIndividual &&
      partida.motivo === "normal"
    ) {
      const idsUsuarios = partida.parceiros
        .map((ws) => partida.idsPorWs.get(ws))
        .filter(Boolean);

      if (idsUsuarios.length > 0) {
        const placeholders = idsUsuarios.map(() => "?").join(",");

        const [rows] = await connection.query(
          `SELECT id, rating
             FROM usuarios
            WHERE id IN (${placeholders})
            FOR UPDATE`,
          idsUsuarios
        );

        rows.forEach((row) => {
          ratings.set(
            String(row.id),
            Number(row.rating) || rankingService.RATING_INICIAL
          );
        });
      }
    }

    for (const ws of [...partida.parceiros]) {
      const usuarioId = partida.idsPorWs.get(ws);

      if (!usuarioId) {
        console.log(`   ⚠️ Pulando ws sem usuário mapeado`);
        continue;
      }

      await processarJogadorNoFim(
        connection,
        partidaId,
        partida,
        usuarioId,
        ws,
        ratings,
        vencedorId
      );
    }

    for (const usuarioId of [...partida.usuariosQueSairam]) {
      await processarJogadorQueSaiu(
        connection,
        partidaId,
        partida,
        usuarioId
      );
    }

    await connection.execute(
      `UPDATE partidas
          SET status = ?, finalizada_em = NOW()
        WHERE id = ?`,
      [statusFinal, partidaId]
    );

    await connection.commit();

    console.log(
      `✅ Partida finalizada no banco: ${partidaId} | ` +
      `status: ${statusFinal} | ` +
      `motivo: ${partida.motivo}`
    );
  } catch (erro) {
    console.error(`❌ ROLLBACK em ${partidaId}: ${erro.message}`);
    console.error(erro.stack);
    await connection.rollback();
    throw erro;
  } finally {
    connection.release();
  }
}

async function processarJogadorNoFim(
  connection,
  partidaId,
  partida,
  usuarioId,
  ws,
  ratings,
  vencedorId
) {
  const estatisticas =
    partida.estatisticas.get(ws) || {
      respostasTotais: 0,
      respostasCorretas: 0,
      sequenciaAtual: 0,
      melhorSequencia: 0
    };

  const pontuacao = Number(partida.pontuacoes.get(ws)) || 0;

  let resultado;
  let contarNoRanking = false;

  if (partida.partidaIndividual) {
    resultado = "individual";
    contarNoRanking = true;
  } else if (partida.motivo === "adversarioDesconectado") {
    resultado = "vitoria";
    contarNoRanking = false;
  } else if (partida.motivo === "normal") {
    if (vencedorId === null) resultado = "empate";
    else resultado = usuarioId === vencedorId ? "vitoria" : "derrota";
    contarNoRanking = true;
  } else {
    resultado = "abandono";
    contarNoRanking = false;
  }

  console.log(
    `🔍 [processarJogadorNoFim] partida=${partidaId}, ` +
    `usuario=${usuarioId}, motivo="${partida.motivo}", ` +
    `individual=${partida.partidaIndividual}, ` +
    `resultado="${resultado}", contarNoRanking=${contarNoRanking}`
  );

  let ratingAnterior = null;
  let ratingNovo = null;
  let variacaoRating = 0;

  if (!partida.partidaIndividual && partida.motivo === "normal") {
    const adversarioWs = partida.parceiros.find(
      (outro) => outro !== ws
    );

    const adversarioId = adversarioWs
      ? partida.idsPorWs.get(adversarioWs)
      : null;

    if (adversarioId) {
      const ratingJogador = rankingService.obterRating(
        ratings,
        usuarioId
      );

      const ratingOponente = rankingService.obterRating(
        ratings,
        adversarioId
      );

      const resultadoRanking = rankingService.calcularResultado(
        Number(partida.pontuacoes.get(ws)) || 0,
        Number(partida.pontuacoes.get(adversarioWs)) || 0
      );

      const ajuste = rankingService.calcularVariacaoElo(
        ratingJogador,
        ratingOponente,
        resultadoRanking
      );

      ratingAnterior = ajuste.ratingAnterior;
      ratingNovo = ajuste.ratingNovo;
      variacaoRating = ajuste.variacaoRating;

      ratings.set(String(usuarioId), ratingNovo);

      // 🆕 Guarda o resultado pra enviar no "fim"
      partida.resultadosRating.set(ws, {
        ratingAnterior,
        ratingNovo,
        variacaoRating
      });
    }
  }

  await connection.execute(
    `UPDATE partida_jogadores
        SET pontuacao = ?,
            resultado = ?,
            respostas_totais = ?,
            respostas_corretas = ?,
            melhor_sequencia = ?,
            rating_anterior = ?,
            rating_novo = ?,
            variacao_rating = ?,
            saiu_em = NOW()
      WHERE partida_id = ? AND usuario_id = ?`,
    [
      pontuacao,
      resultado,
      estatisticas.respostasTotais,
      estatisticas.respostasCorretas,
      estatisticas.melhorSequencia,
      ratingAnterior,
      ratingNovo,
      variacaoRating,
      partidaId,
      usuarioId
    ]
  );

  if (contarNoRanking) {
    console.log(
      `🟢 [processarJogadorNoFim] VAI chamar atualizarEstatisticasUsuario para ${usuarioId}`
    );

    await atualizarEstatisticasUsuario(
      connection,
      usuarioId,
      partida,
      resultado,
      pontuacao,
      estatisticas,
      ratingNovo,
      variacaoRating
    );
  } else {
    console.log(
      `🟡 [processarJogadorNoFim] NÃO vai atualizar (contarNoRanking=false) para ${usuarioId}, motivo=${partida.motivo}`
    );
  }
}

async function processarJogadorQueSaiu(
  connection,
  partidaId,
  partida,
  usuarioId
) {
  await connection.execute(
    `UPDATE partida_jogadores
        SET resultado = 'abandono',
            saiu_em = NOW()
      WHERE partida_id = ? AND usuario_id = ?`,
    [partidaId, usuarioId]
  );

  console.log(
    `Jogador ${usuarioId} marcado como 'abandono' na partida ${partidaId}`
  );
}

async function atualizarEstatisticasUsuario(
  connection,
  usuarioId,
  partida,
  resultado,
  pontuacao,
  estatisticas,
  ratingNovo,
  variacaoRating
) {
  console.log(
    `🟢 [atualizarStats] INICIO: usuario=${usuarioId}, resultado=${resultado}, pontos=${pontuacao}, motivo=${partida.motivo}, individual=${partida.partidaIndividual}`
  );

  const incVitoria = resultado === "vitoria" ? 1 : 0;
  const incDerrota = resultado === "derrota" ? 1 : 0;
  const incEmpate = resultado === "empate" ? 1 : 0;

  const temRating =
    !partida.partidaIndividual && ratingNovo !== null;

  // ============================================================
  // UPDATE usuarios
  // ============================================================
  let sql = `
    UPDATE usuarios
       SET pontos = pontos + ?,
           partidas_jogadas = partidas_jogadas + 1,
           partidas_vencidas = partidas_vencidas + ?,
           partidas_perdidas = partidas_perdidas + ?,
           partidas_empatadas = partidas_empatadas + ?,
           respostas_totais = respostas_totais + ?,
           respostas_corretas = respostas_corretas + ?,
           sequencia_atual = ?,
           melhor_sequencia = GREATEST(melhor_sequencia, ?),
           ultimo_acesso = NOW()
  `;

  const values = [
    pontuacao,
    incVitoria,
    incDerrota,
    incEmpate,
    estatisticas.respostasTotais,
    estatisticas.respostasCorretas,
    estatisticas.sequenciaAtual,
    estatisticas.melhorSequencia
  ];

  if (temRating) {
    sql += `,
           rating = ?,
           melhor_rating = GREATEST(melhor_rating, ?),
           ultima_variacao_rating = ?,
           partidas_rating = partidas_rating + 1
    `;
    values.push(ratingNovo, ratingNovo, variacaoRating);
  }

  sql += ` WHERE id = ?`;
  values.push(usuarioId);

  try {
    const [resultadoUpdate] = await connection.execute(sql, values);

    console.log(
      `🟢 [atualizarStats] UPDATE usuarios: affectedRows=${resultadoUpdate.affectedRows}, changedRows=${resultadoUpdate.changedRows}`
    );

    if (resultadoUpdate.affectedRows === 0) {
      console.error(
        `❌ [atualizarStats] NENHUMA LINHA ATUALIZADA! Usuário ${usuarioId} não existe em usuarios!`
      );
    }
  } catch (err) {
    console.error(
      `❌ [atualizarStats] ERRO no UPDATE usuarios: ${err.message}`
    );
    console.error(`   code: ${err.code}, sqlState: ${err.sqlState}`);
    throw err;
  }

  // ============================================================
  // Recalcula matéria favorita
  // ============================================================
  try {
    await connection.execute(
      `UPDATE usuarios u
          SET materia_favorita = (
            SELECT um.materia
              FROM usuario_materias um
             WHERE um.usuario_id = u.id
             ORDER BY um.partidas_jogadas DESC, um.pontos DESC
             LIMIT 1
          )
        WHERE u.id = ?`,
      [usuarioId]
    );
  } catch (err) {
    console.error(
      `❌ [atualizarStats] ERRO no UPDATE materia_favorita: ${err.message}`
    );
    throw err;
  }

  // ============================================================
  // UPSERT usuario_materias (sintaxe moderna do MySQL 8.0.20+)
  // ============================================================
  //
  // Correção principal:
  // - Antes: usávamos VALUES(coluna), que está deprecated no
  //   MySQL 8.0.20+ e não aceita placeholders separados.
  // - Agora: usamos alias "AS novo" e referenciamos novo.coluna.
  //
  // Só passamos os placeholders do INSERT (12 no total). O
  // ON DUPLICATE KEY UPDATE reutiliza os valores via "novo".
  //
  // Para o modo individual, NÃO sobrescrevemos rating nem
  // melhor_rating, para não resetar para 1000. Em vez disso,
  // interpolamos o "rating = rating" (mantém o valor atual).
  // ============================================================

  const ratingInit = rankingService.RATING_INICIAL;

  // Monta a parte do rating de forma condicional para o UPDATE.
  // Em partidas individuais, preserva o rating atual.
  const ratingUpdateClause = temRating
    ? `
        rating                 = novo.rating,
        melhor_rating          = GREATEST(usuario_materias.melhor_rating, novo.melhor_rating),
        ultima_variacao_rating = novo.ultima_variacao_rating,
        partidas_rating        = usuario_materias.partidas_rating + novo.partidas_rating,
      `
    : `
        rating                 = usuario_materias.rating,
        melhor_rating          = usuario_materias.melhor_rating,
        ultima_variacao_rating = usuario_materias.ultima_variacao_rating,
        partidas_rating        = usuario_materias.partidas_rating,
      `;

  const insertSql = `
    INSERT INTO usuario_materias (
      usuario_id, materia,
      partidas_jogadas, partidas_vencidas,
      partidas_perdidas, partidas_empatadas,
      pontos, respostas_totais, respostas_corretas,
      rating, melhor_rating, ultima_variacao_rating, partidas_rating
    ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS novo
    ON DUPLICATE KEY UPDATE
      partidas_jogadas   = usuario_materias.partidas_jogadas + 1,
      partidas_vencidas  = usuario_materias.partidas_vencidas + novo.partidas_vencidas,
      partidas_perdidas  = usuario_materias.partidas_perdidas + novo.partidas_perdidas,
      partidas_empatadas = usuario_materias.partidas_empatadas + novo.partidas_empatadas,
      pontos             = usuario_materias.pontos + novo.pontos,
      respostas_totais   = usuario_materias.respostas_totais + novo.respostas_totais,
      respostas_corretas = usuario_materias.respostas_corretas + novo.respostas_corretas,
      ${ratingUpdateClause}
      atualizado_em      = NOW()
  `;

  try {
    const [resultadoUpsert] = await connection.execute(insertSql, [
      usuarioId,
      partida.materia,
      incVitoria,
      incDerrota,
      incEmpate,
      pontuacao,
      estatisticas.respostasTotais,
      estatisticas.respostasCorretas,
      temRating ? ratingNovo : ratingInit,
      temRating ? ratingNovo : ratingInit,
      temRating ? variacaoRating : 0,
      temRating ? 1 : 0
    ]);

    console.log(
      `🟢 [atualizarStats] UPSERT usuario_materias: affectedRows=${resultadoUpsert.affectedRows}, warningStatus=${resultadoUpsert.warningStatus}`
    );
  } catch (err) {
    console.error(
      `❌ [atualizarStats] ERRO no UPSERT usuario_materias: ${err.message}`
    );
    console.error(`   code: ${err.code}, sqlState: ${err.sqlState}`);
    throw err;
  }
}

/* ============================================================
 * Finalização
 * ============================================================ */

async function finalizarPartida(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || partida.encerrando) {
    return;
  }

  partida.encerrando = true;
  partida.motivo = "normal";

  limparTimer(partida);

  console.log(
    `Finalizando partida: ${partidaId} | ` +
    `individual: ${partida.partidaIndividual}`
  );

  try {
    await finalizarPartidaNoBanco(partidaId, partida);
  } catch (erro) {
    console.error("Erro ao finalizar partida no banco:", erro);
  }

  partida.parceiros.forEach((ws) => {
    const adversario = partida.parceiros.find(
      (outro) => outro !== ws
    );

    // 🆕 Pega o resultado do ELO (se houver — pode ser null em single)
    const resultado = partida.resultadosRating.get(ws) || null;

    // 🆕 Faixas antes/depois (usando a função do rankingService)
    const faixaAntes = resultado
      ? rankingService.obterFaixa(resultado.ratingAnterior).nome
      : null;

    const faixaDepois = resultado
      ? rankingService.obterFaixa(resultado.ratingNovo).nome
      : null;


    console.log('📤 Enviando fim com nomes:', {
      jogador: partida.nomesPorWs.get(ws),
      oponente: adversario ? partida.nomesPorWs.get(adversario) : null
    });
    enviarJson(ws, {
      tipo: "fim",
      partidaId,
      motivo: partida.partidaIndividual
        ? "partidaIndividual"
        : "partidaFinalizada",
      partidaIndividual: partida.partidaIndividual,
      pontuacaoJogador:
        Number(partida.pontuacoes.get(ws)) || 0,
      pontuacaoOponente: adversario
        ? Number(partida.pontuacoes.get(adversario)) || 0
        : 0,

      // 🆕 Nomes reais
      nomeJogador: partida.nomesPorWs.get(ws) || null,
      nomeOponente: adversario
        ? partida.nomesPorWs.get(adversario) || null
        : null,

      // 🆕 Novos campos de rating
      ratingAnterior: resultado ? resultado.ratingAnterior : null,
      ratingNovo: resultado ? resultado.ratingNovo : null,
      variacaoRating: resultado ? resultado.variacaoRating : null,
      faixaAntes,
      faixaDepois,
      subiuFaixa: faixaAntes !== null && faixaAntes !== faixaDepois
    });
  });

  limparPartida(partidaId);
}

/* ============================================================
 * Limpeza
 * ============================================================ */

function limparPartida(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida) return;

  limparTimer(partida);

  partida.parceiros.forEach((ws) => {
    partidasPorUsuario.delete(ws);
    userMateria.delete(ws);
    limparTimerSingle(ws);
  });

  paresAtivos.delete(partidaId);
}

/* ============================================================
 * Desconexão
 * ============================================================ */

function removerJogador(ws) {
  limparTimerSingle(ws);
  userMateria.delete(ws);

  const partidaId = partidasPorUsuario.get(ws);

  if (!partidaId) return;

  const partida = paresAtivos.get(partidaId);

  if (!partida) {
    partidasPorUsuario.delete(ws);
    return;
  }

  const usuarioQueSaiu =
    obterUsuarioConectado(ws) || null;

  if (usuarioQueSaiu) {
    partida.usuariosQueSairam.add(usuarioQueSaiu.id);

    console.log(
      `Jogador ${usuarioQueSaiu.nome} abandonou a partida ${partidaId}`
    );
  }

  partida.parceiros = partida.parceiros.filter(
    (jogador) => jogador !== ws
  );

  partida.respostasRecebidas.delete(ws);
  // NÃO deletar pontuacoes/estatisticas/idsPorWs aqui,
  // para que os dados ainda possam ser persistidos.
  partidasPorUsuario.delete(ws);

  if (partida.encerrando) {
    return;
  }

  partida.encerrando = true;
  limparTimer(partida);

  const jogadorRestante = partida.parceiros[0];

  if (jogadorRestante) {
    partida.motivo = "adversarioDesconectado";

    finalizarPartidaNoBanco(partidaId, partida)
      .then(() => {
        console.log(`Abandono persistido: ${partidaId}`);
      })
      .catch((erro) => {
        console.error(
          `Erro ao persistir abandono ${partidaId}:`,
          erro
        );
      })
      .finally(() => {
        enviarJson(jogadorRestante, {
          tipo: "fim",
          partidaId,
          motivo: "adversarioDesconectado",
          partidaIndividual: false,
          pontuacaoJogador:
            Number(partida.pontuacoes.get(jogadorRestante)) || 0,
          pontuacaoOponente: 0,

  // 🆕 Nomes reais
  nomeJogador: partida.nomesPorWs.get(jogadorRestante) || null,
          nomeOponente: null
        });

        limparPartida(partidaId);
      });
  } else {
    partida.motivo = "ambosDesconectaram";

    finalizarPartidaNoBanco(partidaId, partida)
      .then(() => {
        console.log(`Cancelamento persistido: ${partidaId}`);
      })
      .catch((erro) => {
        console.error(
          `Erro ao persistir cancelamento ${partidaId}:`,
          erro
        );
      })
      .finally(() => {
        limparPartida(partidaId);
      });
  }
}

/* ============================================================
 * Matchmaking
 * ============================================================ */
function tentarCriarPartidaMultiplayer(materia) {
  const fila = Array.from(userMateria.entries())
    .filter(([ws, materiaEscolhida]) => {
      return (
        materiaEscolhida === materia &&
        !partidasPorUsuario.has(ws) &&
        usuariosConectados.has(ws) &&
        ws.readyState === WebSocket.OPEN
      );
    })
    .map(([ws]) => ws);

  if (fila.length < 2) return false;

  const jogadores = fila.slice(0, 2);

  const partidaId = criarPartida(jogadores, materia);

  const partida = paresAtivos.get(partidaId);

  jogadores.forEach((ws) => {
    const adversario = jogadores.find((outro) => outro !== ws);

    enviarJson(ws, {
      tipo: "parFormado",
      partidaId,
      materia,
      tempoAbertura: 5,

      // 🆕 Nomes dos jogadores
      nomeJogador: partida.nomesPorWs.get(ws) || null,
      nomeOponente: adversario
        ? partida.nomesPorWs.get(adversario) || null
        : null
    });
  });

  return true;
}


function agendarPartidaSingle(ws, materia) {
  limparTimerSingle(ws);

  const timer = setTimeout(() => {
    timersSingle.delete(ws);

    if (
      ws.readyState !== WebSocket.OPEN ||
      partidasPorUsuario.has(ws) ||
      userMateria.get(ws) !== materia
    ) {
      return;
    }

    const partidaId = criarPartida([ws], materia);

    enviarJson(ws, {
      tipo: "status",
      mensagem:
        "Nenhum adversário encontrado. " +
        "Partida individual iniciada!",
      partidaId
    });

    console.log(`Partida individual criada: ${partidaId}`);
  }, TEMPO_ESPERA_SINGLE_MS);

  timersSingle.set(ws, timer);
}

/* ============================================================
 * Conexão WebSocket
 * ============================================================ */

wss.on("connection", (ws) => {
  console.log("Novo cliente conectado.");

  enviarJson(ws, {
    tipo: "status",
    mensagem: "Escolha a matéria..."
  });

  ws.on("message", async (message) => {
    console.log("Mensagem recebida:", message.toString());

    try {
      const data = JSON.parse(message.toString());

      if (data.tipo === "identificarUsuario") {
        await identificarUsuario(ws, data);
        return;
      }

      if (data.tipo === "reset") {
        removerJogador(ws);

        enviarJson(ws, {
          tipo: "status",
          mensagem: "Sessão reiniciada. Escolha a matéria..."
        });

        return;
      }

      if (!obterUsuarioConectado(ws)) {
        enviarJson(ws, {
          tipo: "erro",
          mensagem: "Identifique o usuário antes de continuar."
        });

        return;
      }

      if (data.materia) {
        if (partidasPorUsuario.has(ws)) {
          enviarJson(ws, {
            tipo: "status",
            mensagem: "Você já está em uma partida ativa."
          });

          return;
        }

        const materia = String(data.materia).trim().toLowerCase();

        userMateria.set(ws, materia);

        enviarJson(ws, {
          tipo: "status",
          mensagem:
            `Você escolheu ${materia}, ` +
            "aguardando outro usuário..."
        });

        const partidaCriada = tentarCriarPartidaMultiplayer(materia);

        if (!partidaCriada) {
          agendarPartidaSingle(ws, materia);
        }

        return;
      }

      if (data.tipo === "novaPergunta" && data.partidaId) {
        registrarInicioDaPartida(ws, data.partidaId);
        return;
      }

      if (data.tipo === "resposta" && data.partidaId) {
        registrarResposta(ws, data);
        return;
      }

      enviarJson(ws, {
        tipo: "erro",
        mensagem: "Mensagem não reconhecida."
      });
    } catch (erro) {
      console.error("Erro ao processar mensagem:", erro);

      enviarJson(ws, {
        tipo: "erro",
        mensagem: "Mensagem inválida recebida."
      });
    }
  });

  ws.on("close", (codigo, motivo) => {
    const usuario = obterUsuarioConectado(ws);

    console.log(
      "Cliente desconectado:",
      usuario || "não identificado",
      "| código:", codigo,
      "| motivo:", motivo?.toString() || ""
    );

    removerJogador(ws);
    usuariosConectados.delete(ws);
  });

  ws.on("error", (erro) => {
    console.error("Erro no WebSocket:", erro);
  });
});

console.log(`Servidor WebSocket rodando na porta ${PORT}`);