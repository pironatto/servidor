const WebSocket = require("ws");
const mysql = require("mysql2/promise");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 3000);
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

const wss = new WebSocket.Server({ port: PORT });

const userMateria = new Map();
const paresAtivos = new Map();
const partidasPorUsuario = new Map();
const timersSingle = new Map();

function esperar(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function enviarJson(ws, dados) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(dados));
  }
}

function enviarParaJogadores(partida, dados) {
  if (!partida) {
    return;
  }

  const payload =
    typeof dados === "string"
      ? dados
      : JSON.stringify(dados);

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

function criarPartida(jogadores, materia) {
  const partidaId = crypto.randomUUID();

  const partida = {
    parceiros: jogadores,
    materia,
    perguntasUsadas: new Set(),

    perguntaAtual: null,
    numeroPergunta: 0,

    // ws -> resposta recebida
    respostasRecebidas: new Map(),

    aguardandoInicio: new Set(jogadores),

    // ws -> pontuação acumulada
    pontuacoes: new Map(),

    timer: null,
    processandoRespostas: false,
    encerrando: false
  };

  jogadores.forEach((ws) => {
    limparTimerSingle(ws);

    userMateria.delete(ws);
    partidasPorUsuario.set(ws, partidaId);
    partida.pontuacoes.set(ws, 0);
  });

  paresAtivos.set(partidaId, partida);

  console.log(
    `Partida criada: ${partidaId} | ` +
    `matéria: ${materia} | ` +
    `jogadores: ${jogadores.length}`
  );

  return partidaId;
}

function limparPartida(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida) {
    return;
  }

  limparTimer(partida);

  partida.parceiros.forEach((ws) => {
    partidasPorUsuario.delete(ws);
    userMateria.delete(ws);
    limparTimerSingle(ws);
  });

  paresAtivos.delete(partidaId);
}

function removerJogador(ws) {
  limparTimerSingle(ws);
  userMateria.delete(ws);

  const partidaId = partidasPorUsuario.get(ws);

  if (!partidaId) {
    return;
  }

  const partida = paresAtivos.get(partidaId);

  if (!partida) {
    partidasPorUsuario.delete(ws);
    return;
  }

  partida.parceiros = partida.parceiros.filter(
    (usuario) => usuario !== ws
  );

  partida.respostasRecebidas.delete(ws);
  partida.pontuacoes.delete(ws);
  partidasPorUsuario.delete(ws);

  if (partida.parceiros.length > 0) {
    enviarParaJogadores(partida, {
      tipo: "fim",
      partidaId,
      motivo: "adversarioDesconectado"
    });
  }

  limparPartida(partidaId);
}

function iniciarTimerDaPergunta(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida) {
    return;
  }

  limparTimer(partida);

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
      processarResposta(ws, atual, "");
    });

    await verificarRespostasDaPartida(partidaId);
  }, TEMPO_PERGUNTA_MS);
}

async function enviarPergunta(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || partida.encerrando) {
    return;
  }

  if (partida.numeroPergunta >= TOTAL_PERGUNTAS) {
    await finalizarPartida(partidaId);
    return;
  }

  if (
    partida.perguntaAtual !== null ||
    partida.processandoRespostas
  ) {
    console.warn(
      `Pergunta não enviada: partida ${partidaId} está ocupada.`
    );

    return;
  }

  try {
    const idsUsados = Array.from(partida.perguntasUsadas);
    let rows;

    if (idsUsados.length === 0) {
      [rows] = await db.query(
        `
        SELECT id, materia, pergunta, r1, r2, r3, r4, correta
        FROM questoes
        WHERE materia = ?
        ORDER BY RAND()
        LIMIT 1
        `,
        [partida.materia]
      );
    } else {
      [rows] = await db.query(
        `
        SELECT id, materia, pergunta, r1, r2, r3, r4, correta
        FROM questoes
        WHERE materia = ?
          AND id NOT IN (?)
        ORDER BY RAND()
        LIMIT 1
        `,
        [partida.materia, idsUsados]
      );
    }

    if (!rows || rows.length === 0) {
      await finalizarPartida(partidaId);
      return;
    }

    const row = rows[0];

    partida.perguntasUsadas.add(row.id);
    partida.numeroPergunta += 1;
    partida.respostasRecebidas.clear();

    // A resposta correta fica somente no servidor.
    partida.perguntaAtual = {
      id: row.id,
      correta: String(row.correta).trim().toUpperCase(),
      inicio: Date.now()
    };

    const payload = {
      tipo: "itens",
      partidaId,

      // A resposta correta NÃO é enviada ao cliente.
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
      inicio: partida.perguntaAtual.inicio
    };

    console.log(
      `Enviando pergunta ${partida.numeroPergunta}/` +
      `${TOTAL_PERGUNTAS}: ${partidaId}`
    );

    enviarParaJogadores(partida, payload);
    iniciarTimerDaPergunta(partidaId);
  } catch (erro) {
    console.error(
      "Erro ao consultar banco de perguntas:",
      erro
    );

    await finalizarPartida(partidaId);
  }
}

function calcularPontos(partida) {
  if (!partida.perguntaAtual) {
    return 0;
  }

  const tempoDecorrido =
    Date.now() - partida.perguntaAtual.inicio;

  const tempoRestante =
    TEMPO_PERGUNTA_MS - tempoDecorrido;

  return Math.max(0, tempoRestante / 1000);
}

function processarResposta(ws, partida, resposta) {
  if (!partida || !partida.perguntaAtual) {
    return;
  }

  if (partida.respostasRecebidas.has(ws)) {
    return;
  }

  const respostaNormalizada =
    typeof resposta === "string"
      ? resposta.trim().toUpperCase()
      : "";

  const acertou =
    respostaNormalizada !== "" &&
    respostaNormalizada === partida.perguntaAtual.correta;

  const pontos = acertou
    ? calcularPontos(partida)
    : 0;

  const pontuacaoAnterior =
    Number(partida.pontuacoes.get(ws)) || 0;

  const pontuacaoAtual =
    pontuacaoAnterior + pontos;

  partida.respostasRecebidas.set(
    ws,
    respostaNormalizada
  );

  partida.pontuacoes.set(
    ws,
    pontuacaoAtual
  );

  enviarJson(ws, {
    tipo: "resultadoResposta",
    partidaId: encontrarPartidaId(partida),
    resposta: respostaNormalizada,
    correta: partida.perguntaAtual.correta,
    acertou,
    pontos,
    pontuacaoTotal: pontuacaoAtual
  });

  const adversario = partida.parceiros.find(
    (outro) => outro !== ws
  );

  if (adversario) {
    enviarJson(adversario, {
      tipo: "pontuacaoOponente",
      partidaId: encontrarPartidaId(partida),
      pontos: pontuacaoAtual
    });
  }

  console.log(
    `Resposta validada: ${respostaNormalizada || "SEM RESPOSTA"} | ` +
    `acertou: ${acertou} | pontos: ${pontos.toFixed(1)}`
  );
}

function encontrarPartidaId(partida) {
  for (const [partidaId, partidaAtual] of paresAtivos.entries()) {
    if (partidaAtual === partida) {
      return partidaId;
    }
  }

  return "";
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
    console.log(
      `Respostas: ${partida.respostasRecebidas.size}/` +
      `${partida.parceiros.length}`
    );

    return;
  }

  partida.processandoRespostas = true;
  partida.perguntaAtual = null;

  limparTimer(partida);

  console.log(
    `Todos responderam: ${partidaId}. ` +
    `Aguardando ${TEMPO_FEEDBACK_MS}ms.`
  );

  await esperar(TEMPO_FEEDBACK_MS);

  const atual = paresAtivos.get(partidaId);

  if (!atual || atual.encerrando) {
    return;
  }

  atual.respostasRecebidas.clear();
  atual.processandoRespostas = false;

  await enviarPergunta(partidaId);
}

function registrarInicioDaPartida(ws, partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (
    !partida ||
    !partida.parceiros.includes(ws)
  ) {
    return;
  }

  if (!partida.aguardandoInicio.has(ws)) {
    console.log(
      `Solicitação inicial duplicada ignorada: ${partidaId}`
    );

    return;
  }

  partida.aguardandoInicio.delete(ws);

  console.log(
    `Jogadores prontos: ` +
    `${partida.parceiros.length - partida.aguardandoInicio.size}/` +
    `${partida.parceiros.length}`
  );

  if (partida.aguardandoInicio.size === 0) {
    enviarPergunta(partidaId);
  }
}

function registrarResposta(ws, data) {
  const partida = paresAtivos.get(data.partidaId);

  if (!partida || partida.encerrando) {
    return;
  }

  if (!partida.parceiros.includes(ws)) {
    return;
  }

  if (
    partida.perguntaAtual === null ||
    partida.processandoRespostas
  ) {
    return;
  }

  if (partida.respostasRecebidas.has(ws)) {
    console.log("Resposta duplicada ignorada.");
    return;
  }

  processarResposta(
    ws,
    partida,
    data.resposta
  );

  verificarRespostasDaPartida(data.partidaId);
}

async function finalizarPartida(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || partida.encerrando) {
    return;
  }

  partida.encerrando = true;
  limparTimer(partida);

  console.log(`Finalizando partida: ${partidaId}`);

  partida.parceiros.forEach((usuario) => {
    const adversario = partida.parceiros.find(
      (outro) => outro !== usuario
    );

    enviarJson(usuario, {
      tipo: "fim",
      partidaId,

      pontuacaoJogador:
        Number(partida.pontuacoes.get(usuario)) || 0,

      pontuacaoOponente: adversario
        ? Number(partida.pontuacoes.get(adversario)) || 0
        : 0
    });
  });

  limparPartida(partidaId);
}

function tentarCriarPartidaMultiplayer(materia) {
  const fila = Array.from(userMateria.entries())
    .filter(([ws, materiaEscolhida]) => {
      return (
        materiaEscolhida === materia &&
        !partidasPorUsuario.has(ws) &&
        ws.readyState === WebSocket.OPEN
      );
    })
    .map(([ws]) => ws);

  if (fila.length < 2) {
    return false;
  }

  const jogadores = fila.slice(0, 2);
  const partidaId = criarPartida(
    jogadores,
    materia
  );

  jogadores.forEach((ws) => {
    enviarJson(ws, {
      tipo: "parFormado",
      partidaId,
      materia
    });
  });

  return true;
}

function agendarPartidaSingle(ws, materia) {
  limparTimerSingle(ws);

  const timer = setTimeout(() => {
    timersSingle.delete(ws);

    if (ws.readyState !== WebSocket.OPEN) {
      return;
    }

    if (partidasPorUsuario.has(ws)) {
      return;
    }

    if (userMateria.get(ws) !== materia) {
      return;
    }

    const partidaId = criarPartida(
      [ws],
      materia
    );

    enviarJson(ws, {
      tipo: "status",
      mensagem:
        "Nenhum adversário encontrado. " +
        "Partida individual iniciada!",
      partidaId
    });

    console.log(
      `Partida single criada: ${partidaId}`
    );
  }, TEMPO_ESPERA_SINGLE_MS);

  timersSingle.set(ws, timer);
}

wss.on("connection", (ws) => {
  console.log("Novo cliente conectado.");

  enviarJson(ws, {
    tipo: "status",
    mensagem: "Escolha a matéria..."
  });

  ws.on("message", (message) => {
    console.log(
      "Mensagem recebida:",
      message.toString()
    );

    try {
      const data = JSON.parse(
        message.toString()
      );

      if (data.tipo === "reset") {
        removerJogador(ws);

        enviarJson(ws, {
          tipo: "status",
          mensagem:
            "Sessão reiniciada. Escolha a matéria..."
        });

        return;
      }

      if (data.materia) {
        if (partidasPorUsuario.has(ws)) {
          enviarJson(ws, {
            tipo: "status",
            mensagem:
              "Você já está em uma partida ativa."
          });

          return;
        }

        const materia = String(data.materia)
          .trim()
          .toLowerCase();

        userMateria.set(ws, materia);

        enviarJson(ws, {
          tipo: "status",
          mensagem:
            `Você escolheu ${materia}, ` +
            "aguardando outro usuário..."
        });

        const partidaCriada =
          tentarCriarPartidaMultiplayer(materia);

        if (!partidaCriada) {
          agendarPartidaSingle(ws, materia);
        }

        return;
      }

      if (
        data.tipo === "novaPergunta" &&
        data.partidaId
      ) {
        registrarInicioDaPartida(
          ws,
          data.partidaId
        );

        return;
      }

      if (
        data.tipo === "resposta" &&
        data.partidaId
      ) {
        registrarResposta(ws, data);
        return;
      }

      console.warn(
        "Mensagem não reconhecida:",
        data
      );
    } catch (erro) {
      console.error(
        "Erro ao processar mensagem:",
        erro
      );
    }
  });

  ws.on("close", () => {
    console.log("Cliente desconectado.");
    removerJogador(ws);
  });

  ws.on("error", (erro) => {
    console.error(
      "Erro no WebSocket:",
      erro
    );
  });
});

console.log(
  `Servidor WebSocket rodando na porta ${PORT}`
);