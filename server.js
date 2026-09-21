const WebSocket = require("ws");
const mysql = require("mysql2/promise");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 3000);
const TEMPO_PERGUNTA_MS = 10000;
const TEMPO_FEEDBACK_MS = 2000;
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

function esperar(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function enviarParaJogadores(partida, payload) {
  if (!partida || !Array.isArray(partida.parceiros)) {
    return;
  }

  partida.parceiros.forEach((usuario) => {
    if (usuario && usuario.readyState === WebSocket.OPEN) {
      usuario.send(payload);
    }
  });
}

function enviarJson(usuario, dados) {
  if (usuario && usuario.readyState === WebSocket.OPEN) {
    usuario.send(JSON.stringify(dados));
  }
}

function limparTimer(partida) {
  if (partida && partida.timer) {
    clearTimeout(partida.timer);
    partida.timer = null;
  }
}

function limparPartida(partidaId, enviarFim = false) {
  const partida = paresAtivos.get(partidaId);

  if (!partida) {
    return;
  }

  limparTimer(partida);

  if (enviarFim) {
    enviarParaJogadores(
      partida,
      JSON.stringify({
        tipo: "fim",
        partidaId
      })
    );
  }

  partida.parceiros.forEach((usuario) => {
    partidasPorUsuario.delete(usuario);
    userMateria.delete(usuario);
  });

  paresAtivos.delete(partidaId);
}

function removerJogador(ws) {
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

  partidasPorUsuario.delete(ws);

  if (partida.parceiros.length === 0) {
    limparPartida(partidaId, false);
    return;
  }

  enviarParaJogadores(
    partida,
    JSON.stringify({
      tipo: "fim",
      partidaId,
      motivo: "adversarioDesconectado"
    })
  );

  limparPartida(partidaId, false);
}

function criarPartida(jogadores, materia) {
  const partidaId = crypto.randomUUID();

  const partida = {
    parceiros: jogadores,
    materia,
    perguntasUsadas: new Set(),
    perguntaAtual: null,
    numeroPergunta: 0,
    respostasRecebidas: new Set(),
    aguardandoInicio: new Set(jogadores),
    pontuacoes: new Map(),
    timer: null,
    processandoRespostas: false,
    encerrando: false
  };

  paresAtivos.set(partidaId, partida);

  jogadores.forEach((usuario) => {
    userMateria.delete(usuario);
    partidasPorUsuario.set(usuario, partidaId);
  });

  console.log(
    `Partida criada: ${partidaId} | matéria: ${materia} | jogadores: ${jogadores.length}`
  );

  return partidaId;
}

function iniciarTimerDaPergunta(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida) {
    return;
  }

  limparTimer(partida);

  partida.timer = setTimeout(async () => {
    const partidaAtual = paresAtivos.get(partidaId);

    if (!partidaAtual || partidaAtual.encerrando) {
      return;
    }

    console.log(
      `Tempo esgotado para a pergunta ${partidaAtual.numeroPergunta} da partida ${partidaId}`
    );

    partidaAtual.parceiros.forEach((usuario) => {
      partidaAtual.respostasRecebidas.add(usuario);
    });

    await verificarRespostasDaPartida(partidaId);
  }, TEMPO_PERGUNTA_MS);
}

async function enviarPergunta(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || partida.encerrando) {
    console.warn(`Partida não encontrada ou encerrando: ${partidaId}`);
    return;
  }

  if (partida.numeroPergunta >= TOTAL_PERGUNTAS) {
    finalizarPartida(partidaId);
    return;
  }

  if (partida.perguntaAtual !== null) {
    console.warn(`A partida ${partidaId} já possui uma pergunta ativa.`);
    return;
  }

  try {
    let rows;

    if (partida.perguntasUsadas.size === 0) {
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
      const idsUsados = Array.from(partida.perguntasUsadas);

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
      enviarParaJogadores(
        partida,
        JSON.stringify({
          tipo: "status",
          partidaId,
          mensagem: `Não há perguntas disponíveis para ${partida.materia}.`
        })
      );

      finalizarPartida(partidaId);
      return;
    }

    const row = rows[0];

    partida.perguntasUsadas.add(row.id);
    partida.numeroPergunta += 1;
    partida.perguntaAtual = row.id;
    partida.respostasRecebidas.clear();
    partida.processandoRespostas = false;

    const inicio = Date.now();

    const itens = [
      String(row.id),
      row.materia,
      row.pergunta,
      row.r1,
      row.r2,
      row.r3,
      row.r4,
      row.correta
    ];

    const payload = {
      tipo: "itens",
      partidaId,
      itens,
      tempoTotal: TEMPO_PERGUNTA_MS / 1000,
      inicio
    };

    console.log(
      `Enviando pergunta ${partida.numeroPergunta}/${TOTAL_PERGUNTAS} para a partida ${partidaId}`
    );

    enviarParaJogadores(partida, JSON.stringify(payload));
    iniciarTimerDaPergunta(partidaId);
  } catch (erro) {
    console.error("Erro ao consultar banco de perguntas:", erro);

    enviarParaJogadores(
      partida,
      JSON.stringify({
        tipo: "status",
        partidaId,
        mensagem: "Erro ao carregar a pergunta."
      })
    );
  }
}

async function verificarRespostasDaPartida(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || partida.encerrando) {
    return;
  }

  if (partida.processandoRespostas) {
    return;
  }

  if (partida.respostasRecebidas.size < partida.parceiros.length) {
    console.log(
      `Respostas recebidas: ${partida.respostasRecebidas.size}/${partida.parceiros.length}`
    );
    return;
  }

  partida.processandoRespostas = true;
  limparTimer(partida);

  console.log(
    `Todos responderam à pergunta ${partida.numeroPergunta} da partida ${partidaId}`
  );

  partida.perguntaAtual = null;
  partida.respostasRecebidas.clear();

  console.log(
    `Aguardando ${TEMPO_FEEDBACK_MS / 1000} segundos antes da próxima pergunta.`
  );

  await esperar(TEMPO_FEEDBACK_MS);

  const partidaAtualizada = paresAtivos.get(partidaId);

  if (!partidaAtualizada || partidaAtualizada.encerrando) {
    return;
  }

  partidaAtualizada.processandoRespostas = false;
  await enviarPergunta(partidaId);
}

function registrarInicioDaPartida(ws, partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida) {
    console.warn(`Partida não encontrada ao iniciar: ${partidaId}`);
    return;
  }

  if (!partida.parceiros.includes(ws)) {
    console.warn("Jogador não pertence à partida informada.");
    return;
  }

  if (!partida.aguardandoInicio.has(ws)) {
    console.log("Solicitação inicial duplicada ignorada.");
    return;
  }

  partida.aguardandoInicio.delete(ws);

  console.log(
    `Jogadores prontos: ${partida.parceiros.length - partida.aguardandoInicio.size}/${partida.parceiros.length}`
  );

  if (partida.aguardandoInicio.size === 0) {
    enviarPergunta(partidaId);
  }
}

function registrarResposta(ws, data) {
  const partida = paresAtivos.get(data.partidaId);

  if (!partida || partida.encerrando) {
    console.warn(`Resposta recebida para partida inválida: ${data.partidaId}`);
    return;
  }

  if (!partida.parceiros.includes(ws)) {
    console.warn("Jogador não pertence à partida.");
    return;
  }

  if (partida.perguntaAtual === null) {
    console.warn("Resposta recebida sem pergunta ativa.");
    return;
  }

  if (partida.processandoRespostas) {
    console.log("Resposta recebida enquanto a próxima pergunta está sendo preparada.");
    return;
  }

  if (partida.respostasRecebidas.has(ws)) {
    console.log("Resposta duplicada ignorada.");
    return;
  }

  const resposta =
    typeof data.resposta === "string"
      ? data.resposta.trim().toUpperCase()
      : "";

  partida.respostasRecebidas.add(ws);

  console.log(
    `Resposta recebida: ${resposta || "SEM RESPOSTA"} | ` +
    `${partida.respostasRecebidas.size}/${partida.parceiros.length} | ` +
    `partida ${data.partidaId}`
  );

  verificarRespostasDaPartida(data.partidaId);
}

function finalizarPartida(partidaId) {
  const partida = paresAtivos.get(partidaId);

  if (!partida || partida.encerrando) {
    return;
  }

  partida.encerrando = true;
  limparTimer(partida);

  console.log(`Finalizando partida: ${partidaId}`);

  partida.parceiros.forEach((usuario) => {
    const pontuacaoJogador = Number(partida.pontuacoes.get(usuario)) || 0;

    const adversario = partida.parceiros.find(
      (outroUsuario) => outroUsuario !== usuario
    );

    const pontuacaoOponente = adversario
      ? Number(partida.pontuacoes.get(adversario)) || 0
      : 0;

    enviarJson(usuario, {
      tipo: "fim",
      partidaId,
      pontuacaoJogador,
      pontuacaoOponente
    });
  });

  partida.parceiros.forEach((usuario) => {
    partidasPorUsuario.delete(usuario);
    userMateria.delete(usuario);
  });

  paresAtivos.delete(partidaId);
}

function tentarCriarPartidaMultiplayer(materia) {
  const fila = Array.from(userMateria.entries())
    .filter(([usuario, materiaEscolhida]) => {
      return (
        materiaEscolhida === materia &&
        !partidasPorUsuario.has(usuario) &&
        usuario.readyState === WebSocket.OPEN
      );
    })
    .map(([usuario]) => usuario);

  if (fila.length < 2) {
    return false;
  }

  const jogadores = fila.slice(0, 2);
  const partidaId = criarPartida(jogadores, materia);

  jogadores.forEach((usuario) => {
    enviarJson(usuario, {
      tipo: "parFormado",
      partidaId,
      materia
    });
  });

  return true;
}

function agendarPartidaSingle(ws, materia) {
  setTimeout(() => {
    if (ws.readyState !== WebSocket.OPEN) {
      return;
    }

    if (partidasPorUsuario.has(ws)) {
      return;
    }

    if (userMateria.get(ws) !== materia) {
      return;
    }

    const partidaId = criarPartida([ws], materia);

    enviarJson(ws, {
      tipo: "status",
      mensagem: "Nenhum adversário encontrado. Partida single iniciada!",
      partidaId
    });

    console.log(`Partida single criada: ${partidaId}`);
  }, TEMPO_PERGUNTA_MS);
}

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

      if (data.tipo === "reset") {
        removerJogador(ws);

        enviarJson(ws, {
          tipo: "status",
          mensagem: "Sessão reiniciada. Escolha a matéria..."
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
          mensagem: `Você escolheu ${materia}, aguardando outro usuário...`
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

      if (data.tipo === "pontuacao" && data.partidaId) {
        const partida = paresAtivos.get(data.partidaId);

        if (!partida) {
          return;
        }

        const pontos = Number(data.pontos);

        if (!Number.isFinite(pontos) || pontos < 0) {
          console.warn("Pontuação inválida recebida:", data.pontos);
          return;
        }

        partida.pontuacoes.set(ws, pontos);

        console.log(
          `Pontuação registrada: ${pontos} | partida ${data.partidaId}`
        );

        partida.parceiros.forEach((usuario) => {
          if (usuario !== ws && usuario.readyState === WebSocket.OPEN) {
            usuario.send(
              JSON.stringify({
                tipo: "pontuacaoOponente",
                partidaId: data.partidaId,
                jogadorId: data.jogadorId,
                pontos
              })
            );
          }
        });

        return;
      }

      console.warn("Mensagem não reconhecida:", data);
    } catch (erro) {
      console.error("Erro ao processar mensagem:", erro);
    }
  });

  ws.on("close", () => {
    console.log("Cliente desconectado.");
    removerJogador(ws);
  });

  ws.on("error", (erro) => {
    console.error("Erro no WebSocket:", erro);
  });
});

console.log(`Servidor WebSocket rodando na porta ${PORT}`);