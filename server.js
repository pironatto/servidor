const WebSocket = require("ws");
const crypto = require("crypto");

const persistencia = require("./persistencia");

const db = persistencia.db;

const PORT = Number(process.env.PORT || 3000);

const TEMPO_PERGUNTA_MS = 10000;
const TEMPO_FEEDBACK_MS = 2000;
const TEMPO_ESPERA_SINGLE_MS = 10000;
const TOTAL_PERGUNTAS = 5;

const wss = new WebSocket.Server({
  port: PORT
});

/*
 * WebSocket -> matéria escolhida
 */
const userMateria = new Map();

/*
 * partidaId -> objeto da partida
 */
const paresAtivos = new Map();

/*
 * WebSocket -> partidaId
 */
const partidasPorUsuario = new Map();

/*
 * WebSocket -> timer da partida individual
 */
const timersSingle = new Map();

/*
 * WebSocket -> dados do usuário autenticado
 *
 * Exemplo:
 * ws -> {
 *   id: "G6vFBNjlnhnpAgGyeMfdVxlezMkK",
 *   nome: "Marcelo"
 * }
 */
const usuariosConectados = new Map();

/*
 * Gravações no banco ainda em andamento.
 */
const persistenciasPendentes = new Set();

/*
 * Aguarda alguns milissegundos.
 */
function esperar(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

/*
 * Envia um objeto JSON para um cliente.
 */
function enviarJson(ws, dados) {
  if (
    ws &&
    ws.readyState === WebSocket.OPEN
  ) {
    ws.send(JSON.stringify(dados));
  }
}

/*
 * Envia uma mensagem para todos os jogadores da partida.
 */
function enviarParaJogadores(partida, dados) {
  if (!partida) {
    return;
  }

  const payload =
    typeof dados === "string"
      ? dados
      : JSON.stringify(dados);

  partida.parceiros.forEach((ws) => {
    if (
      ws &&
      ws.readyState === WebSocket.OPEN
    ) {
      ws.send(payload);
    }
  });
}

/*
 * Verifica se o usuário já foi identificado.
 */
function obterUsuarioConectado(ws) {
  return usuariosConectados.get(ws) || null;
}

/*
 * Identifica o usuário pelo ID recebido do Unity.
 */
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
      `
      SELECT id, nome
      FROM usuarios
      WHERE id = ?
      LIMIT 1
      `,
      [usuarioId]
    );

    if (
      !rows ||
      rows.length === 0
    ) {
      console.warn(
        `Usuário não encontrado no banco: ${usuarioId}`
      );

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
      `
      UPDATE usuarios
      SET ultimo_acesso = NOW()
      WHERE id = ?
      `,
      [usuario.id]
    );

    enviarJson(ws, {
      tipo: "usuarioIdentificado",
      usuarioId: String(usuario.id),
      nome: String(usuario.nome)
    });

    console.log(
      `Usuário identificado: ${usuario.nome} ` +
      `(${usuario.id})`
    );

    return true;
  } catch (erro) {
    console.error(
      "Erro ao identificar usuário:",
      erro
    );

    enviarJson(ws, {
      tipo: "erro",
      mensagem:
        "Não foi possível identificar o usuário."
    });

    return false;
  }
}

/*
 * Executa as gravações no banco em série,
 * preservando a ordem dos registros da partida.
 */
function enfileirarPersistencia(
  partida,
  acao,
  descricao
) {
  if (!partida) {
    return Promise.resolve();
  }

  partida.filaPersistencia =
    partida.filaPersistencia
      .then(acao)
      .catch((erro) => {
        console.error(
          `Erro ao ${descricao}:`,
          erro
        );
      });

  const pendente = partida.filaPersistencia;

  persistenciasPendentes.add(pendente);

  pendente.finally(() => {
    persistenciasPendentes.delete(pendente);
  });

  return pendente;
}

/*
 * Espera as gravações em andamento antes de ler
 * estatísticas já consolidadas.
 */
async function aguardarPersistencias() {
  await Promise.allSettled(
    Array.from(persistenciasPendentes)
  );
}

/*
 * Limpa o timer de uma partida multiplayer.
 */
function limparTimer(partida) {
  if (
    partida &&
    partida.timer
  ) {
    clearTimeout(partida.timer);
    partida.timer = null;
  }
}

/*
 * Limpa o timer de uma partida individual.
 */
function limparTimerSingle(ws) {
  const timer = timersSingle.get(ws);

  if (timer) {
    clearTimeout(timer);
  }

  timersSingle.delete(ws);
}

/*
 * Cria uma partida em memória.
 */
function criarPartida(jogadores, materia) {
  const partidaId = crypto.randomUUID();

  const partida = {
    parceiros: jogadores,
    materia,

    partidaIndividual:
      jogadores.length === 1,

    perguntasUsadas: new Set(),

    perguntaAtual: null,
    numeroPergunta: 0,

    respostasRecebidas: new Map(),
    aguardandoInicio: new Set(jogadores),
    pontuacoes: new Map(),
    estatisticas: new Map(),
    usuarios: new Map(),

    filaPersistencia: Promise.resolve(),

    timer: null,
    processandoRespostas: false,
    encerrando: false
  };

  jogadores.forEach((ws) => {
    limparTimerSingle(ws);

    userMateria.delete(ws);

    partidasPorUsuario.set(
      ws,
      partidaId
    );

    partida.pontuacoes.set(ws, 0);

    partida.estatisticas.set(ws, {
      respostasTotais: 0,
      respostasCorretas: 0,
      sequenciaAtual: 0,
      melhorSequencia: 0
    });

    const usuario = obterUsuarioConectado(ws);

    if (usuario) {
      partida.usuarios.set(ws, usuario);
    }
  });

  paresAtivos.set(
    partidaId,
    partida
  );

  enfileirarPersistencia(
    partida,
    () =>
      persistencia.registrarPartida(
        partidaId,
        materia,

        partida.partidaIndividual
          ? "individual"
          : "multiplayer",

        Array.from(
          partida.usuarios.values()
        )
      ),
    "registrar partida"
  );

  console.log(
    `Partida criada: ${partidaId} | ` +
    `matéria: ${materia} | ` +
    `jogadores: ${jogadores.length} | ` +
    `individual: ${partida.partidaIndividual}`
  );

  return partidaId;
}

/*
 * Remove uma partida da memória.
 */
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

/*
 * Remove um jogador de uma partida.
 */
function removerJogador(ws) {
  limparTimerSingle(ws);
  userMateria.delete(ws);

  const partidaId =
    partidasPorUsuario.get(ws);

  if (!partidaId) {
    return;
  }

  const partida =
    paresAtivos.get(partidaId);

  if (!partida) {
    partidasPorUsuario.delete(ws);
    return;
  }

  const partidaEncerrada = partida.encerrando;

  partida.encerrando = true;

  partida.parceiros =
    partida.parceiros.filter(
      (usuario) => usuario !== ws
    );

  partida.respostasRecebidas.delete(ws);

  partidasPorUsuario.delete(ws);

  if (
    partida.parceiros.length > 0
  ) {
    const jogadorRestante =
      partida.parceiros[0];

    enviarJson(jogadorRestante, {
      tipo: "fim",
      partidaId,
      motivo: "adversarioDesconectado",
      partidaIndividual: false,

      pontuacaoJogador:
        Number(
          partida.pontuacoes.get(
            jogadorRestante
          )
        ) || 0,

      pontuacaoOponente: 0
    });
  }

  if (!partidaEncerrada) {
    persistirResultadoFinal(
      partidaId,
      partida,
      partida.parceiros,
      [ws]
    );
  }

  limparPartida(partidaId);
}

/*
 * Inicia o timer da pergunta.
 */
function iniciarTimerDaPergunta(partidaId) {
  const partida =
    paresAtivos.get(partidaId);

  if (!partida) {
    return;
  }

  limparTimer(partida);

  partida.timer = setTimeout(
    async () => {
      const atual =
        paresAtivos.get(partidaId);

      if (
        !atual ||
        atual.encerrando ||
        atual.processandoRespostas ||
        !atual.perguntaAtual
      ) {
        return;
      }

      console.log(
        `Tempo esgotado: pergunta ` +
        `${atual.numeroPergunta} | ` +
        `partida ${partidaId}`
      );

      const jogadoresSemResposta =
        atual.parceiros.filter(
          (ws) =>
            !atual.respostasRecebidas.has(ws)
        );

      jogadoresSemResposta.forEach((ws) => {
        processarResposta(
          ws,
          atual,
          ""
        );
      });

      await verificarRespostasDaPartida(
        partidaId
      );
    },
    TEMPO_PERGUNTA_MS
  );
}

/*
 * Envia uma nova pergunta.
 */
async function enviarPergunta(partidaId) {
  const partida =
    paresAtivos.get(partidaId);

  if (
    !partida ||
    partida.encerrando
  ) {
    return;
  }

  if (
    partida.numeroPergunta >=
    TOTAL_PERGUNTAS
  ) {
    await finalizarPartida(partidaId);
    return;
  }

  if (
    partida.perguntaAtual !== null ||
    partida.processandoRespostas
  ) {
    console.warn(
      `Pergunta não enviada: partida ` +
      `${partidaId} está ocupada.`
    );

    return;
  }

  try {
    const idsUsados =
      Array.from(
        partida.perguntasUsadas
      );

    let rows;

    if (
      idsUsados.length === 0
    ) {
      [rows] = await db.query(
        `
        SELECT
          id,
          materia,
          pergunta,
          r1,
          r2,
          r3,
          r4,
          correta
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
        SELECT
          id,
          materia,
          pergunta,
          r1,
          r2,
          r3,
          r4,
          correta
        FROM questoes
        WHERE materia = ?
          AND id NOT IN (?)
        ORDER BY RAND()
        LIMIT 1
        `,
        [
          partida.materia,
          idsUsados
        ]
      );
    }

    if (
      !rows ||
      rows.length === 0
    ) {
      await finalizarPartida(partidaId);
      return;
    }

    const row = rows[0];

    partida.perguntasUsadas.add(
      row.id
    );

    partida.numeroPergunta += 1;
    partida.respostasRecebidas.clear();

    partida.perguntaAtual = {
      id: row.id,

      correta: String(
        row.correta
      )
        .trim()
        .toUpperCase(),

      inicio: Date.now()
    };

    const payload = {
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

      tempoTotal:
        TEMPO_PERGUNTA_MS / 1000,

      inicio:
        partida.perguntaAtual.inicio
    };

    console.log(
      `Enviando pergunta ` +
      `${partida.numeroPergunta}/` +
      `${TOTAL_PERGUNTAS}: ` +
      `${partidaId}`
    );

    enviarParaJogadores(
      partida,
      payload
    );

    iniciarTimerDaPergunta(
      partidaId
    );
  } catch (erro) {
    console.error(
      "Erro ao consultar banco de perguntas:",
      erro
    );

    await finalizarPartida(partidaId);
  }
}

/*
 * Calcula os pontos com base no tempo restante.
 */
function calcularPontos(partida) {
  if (
    !partida ||
    !partida.perguntaAtual
  ) {
    return 0;
  }

  const tempoDecorrido =
    Date.now() -
    partida.perguntaAtual.inicio;

  const tempoRestante =
    TEMPO_PERGUNTA_MS -
    tempoDecorrido;

  return Math.max(
    0,
    tempoRestante / 1000
  );
}

/*
 * Processa a resposta de um jogador.
 */
function processarResposta(
  ws,
  partida,
  resposta
) {
  if (
    !partida ||
    !partida.perguntaAtual
  ) {
    return;
  }

  if (
    partida.respostasRecebidas.has(ws)
  ) {
    return;
  }

  const respostaNormalizada =
    typeof resposta === "string"
      ? resposta.trim().toUpperCase()
      : "";

  const acertou =
    respostaNormalizada !== "" &&
    respostaNormalizada ===
    partida.perguntaAtual.correta;

  const pontos =
    acertou
      ? calcularPontos(partida)
      : 0;

  const pontuacaoAnterior =
    Number(
      partida.pontuacoes.get(ws)
    ) || 0;

  const pontuacaoAtual =
    pontuacaoAnterior + pontos;

  const partidaId =
    encontrarPartidaId(partida);

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
    partidaId,
    resposta: respostaNormalizada,
    correta: partida.perguntaAtual.correta,
    acertou,
    pontos,
    pontuacaoTotal: pontuacaoAtual
  });

  const adversario =
    partida.parceiros.find(
      (outro) => outro !== ws
    );

  if (adversario) {
    enviarJson(adversario, {
      tipo: "pontuacaoOponente",
      partidaId,

      pontos: pontuacaoAtual
    });
  }

  const usuario =
    partida.usuarios.get(ws) ||
    obterUsuarioConectado(ws);

  const estatisticas =
    partida.estatisticas.get(ws);

  if (estatisticas) {
    estatisticas.respostasTotais += 1;

    if (acertou) {
      estatisticas.respostasCorretas += 1;
      estatisticas.sequenciaAtual += 1;

      estatisticas.melhorSequencia =
        Math.max(
          estatisticas.melhorSequencia,
          estatisticas.sequenciaAtual
        );
    } else {
      estatisticas.sequenciaAtual = 0;
    }
  }

  if (usuario) {
    const perguntaId =
      partida.perguntaAtual.id;

    const tempoRespostaMs =
      Math.min(
        TEMPO_PERGUNTA_MS,

        Math.max(
          0,

          Date.now() -
          partida.perguntaAtual.inicio
        )
      );

    const respostaCorreta =
      partida.perguntaAtual.correta;

    enfileirarPersistencia(
      partida,
      () =>
        persistencia.registrarResposta({
          partidaId,
          usuarioId: usuario.id,
          perguntaId,
          resposta: respostaNormalizada,
          respostaCorreta,
          acertou,
          pontos,
          tempoRespostaMs
        }),
      "registrar resposta"
    );
  }

  console.log(
    `Resposta validada | ` +
    `jogador: ${usuario?.nome || "desconhecido"} | ` +
    `resposta: ` +
    `${respostaNormalizada || "SEM RESPOSTA"} | ` +
    `acertou: ${acertou} | ` +
    `pontos: ${pontos.toFixed(1)}`
  );
}

/*
 * Revela as respostas dos dois jogadores.
 */
function enviarResumoDasRespostas(
  partidaId,
  partida
) {
  if (
    !partida ||
    partida.partidaIndividual ||
    partida.parceiros.length < 2 ||
    !partida.perguntaAtual
  ) {
    return;
  }

  const jogadorA =
    partida.parceiros[0];

  const jogadorB =
    partida.parceiros[1];

  const respostaA =
    partida.respostasRecebidas.get(
      jogadorA
    ) || "";

  const respostaB =
    partida.respostasRecebidas.get(
      jogadorB
    ) || "";

  const respostaCorreta =
    partida.perguntaAtual.correta;

  const acertouA =
    respostaA !== "" &&
    respostaA === respostaCorreta;

  const acertouB =
    respostaB !== "" &&
    respostaB === respostaCorreta;

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

  console.log(
    `Respostas reveladas: ${partidaId} | ` +
    `Jogador A: ` +
    `${respostaA || "SEM RESPOSTA"} | ` +
    `Jogador B: ` +
    `${respostaB || "SEM RESPOSTA"}`
  );
}

/*
 * Encontra o ID da partida em memória.
 */
function encontrarPartidaId(partida) {
  for (
    const [
      partidaId,
      partidaAtual
    ] of paresAtivos.entries()
  ) {
    if (
      partidaAtual === partida
    ) {
      return partidaId;
    }
  }

  return "";
}

/*
 * Verifica se todos os jogadores responderam.
 */
async function verificarRespostasDaPartida(
  partidaId
) {
  const partida =
    paresAtivos.get(partidaId);

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
      `Respostas: ` +
      `${partida.respostasRecebidas.size}/` +
      `${partida.parceiros.length}`
    );

    return;
  }

  partida.processandoRespostas =
    true;

  enviarResumoDasRespostas(
    partidaId,
    partida
  );

  partida.perguntaAtual = null;

  limparTimer(partida);

  console.log(
    `Todos responderam: ${partidaId}. ` +
    `Aguardando ${TEMPO_FEEDBACK_MS}ms.`
  );

  await esperar(
    TEMPO_FEEDBACK_MS
  );

  const atual =
    paresAtivos.get(partidaId);

  if (
    !atual ||
    atual.encerrando
  ) {
    return;
  }

  atual.respostasRecebidas.clear();
  atual.processandoRespostas =
    false;

  await enviarPergunta(
    partidaId
  );
}

/*
 * Registra que o jogador está pronto para iniciar.
 */
function registrarInicioDaPartida(
  ws,
  partidaId
) {
  const partida =
    paresAtivos.get(partidaId);

  if (
    !partida ||
    !partida.parceiros.includes(ws)
  ) {
    return;
  }

  if (
    !partida.aguardandoInicio.has(ws)
  ) {
    console.log(
      `Solicitação inicial duplicada ` +
      `ignorada: ${partidaId}`
    );

    return;
  }

  partida.aguardandoInicio.delete(ws);

  console.log(
    `Jogadores prontos: ${partida.parceiros.length - partida.aguardandoInicio.size
    }/${partida.parceiros.length}`
  );

if (
  partida.aguardandoInicio.size === 0
) {
  enviarPergunta(partidaId);
}
}

/*
 * Registra uma resposta recebida.
 */
function registrarResposta(ws, data) {
  const partida =
    paresAtivos.get(data.partidaId);

  if (
    !partida ||
    partida.encerrando
  ) {
    return;
  }

  if (
    !partida.parceiros.includes(ws)
  ) {
    return;
  }

  if (
    partida.perguntaAtual === null ||
    partida.processandoRespostas
  ) {
    return;
  }

  if (
    partida.respostasRecebidas.has(ws)
  ) {
    console.log(
      "Resposta duplicada ignorada."
    );

    return;
  }

  processarResposta(
    ws,
    partida,
    data.resposta
  );

  verificarRespostasDaPartida(
    data.partidaId
  );
}

/*
 * Finaliza a partida atual.
 */
async function finalizarPartida(partidaId) {
  const partida =
    paresAtivos.get(partidaId);

  if (
    !partida ||
    partida.encerrando
  ) {
    return;
  }

  partida.encerrando = true;

  limparTimer(partida);

  console.log(
    `Finalizando partida: ${partidaId} | ` +
    `individual: ${partida.partidaIndividual}`
  );

  partida.parceiros.forEach((usuario) => {
    const adversario =
      partida.parceiros.find(
        (outro) => outro !== usuario
      );

    enviarJson(usuario, {
      tipo: "fim",
      partidaId,

      motivo:
        partida.partidaIndividual
          ? "partidaIndividual"
          : "partidaFinalizada",

      partidaIndividual:
        partida.partidaIndividual,

      pontuacaoJogador:
        Number(
          partida.pontuacoes.get(usuario)
        ) || 0,

      pontuacaoOponente:
        adversario
          ? Number(
            partida.pontuacoes.get(
              adversario
            )
          ) || 0
          : 0
    });
  });

  persistirResultadoFinal(
    partidaId,
    partida,
    partida.parceiros
  );

  limparPartida(partidaId);
}

/*
 * Monta o resultado de cada jogador e grava
 * o fechamento da partida no banco.
 *
 * abandonaram: jogadores que sairam antes do fim.
 */
function persistirResultadoFinal(
  partidaId,
  partida,
  jogadores,
  abandonaram = []
) {
  const participantes = jogadores
    .concat(abandonaram)
    .filter((ws) => partida.usuarios.has(ws));

  if (participantes.length === 0) {
    enfileirarPersistencia(
      partida,
      () =>
        persistencia.cancelarPartida(partidaId),
      "cancelar partida"
    );

    return;
  }

  const maiorPontuacao = Math.max(
    ...jogadores.map(
      (ws) =>
        Number(partida.pontuacoes.get(ws)) || 0
    ),
    0
  );

  const empate =
    jogadores.filter(
      (ws) =>
        (Number(partida.pontuacoes.get(ws)) || 0) ===
        maiorPontuacao
    ).length > 1;

  const dadosJogadores = participantes.map((ws) => {
    const usuario = partida.usuarios.get(ws);

    const estatisticas =
      partida.estatisticas.get(ws) || {
        respostasTotais: 0,
        respostasCorretas: 0,
        melhorSequencia: 0
      };

    const pontuacao =
      Number(partida.pontuacoes.get(ws)) || 0;

    let resultado;

    if (abandonaram.includes(ws)) {
      resultado = "abandono";
    } else if (partida.partidaIndividual) {
      resultado = "individual";
    } else if (empate) {
      resultado = "empate";
    } else {
      resultado =
        pontuacao === maiorPontuacao
          ? "vitoria"
          : "derrota";
    }

    return {
      id: usuario.id,
      pontuacao,
      resultado,
      respostasTotais: estatisticas.respostasTotais,
      respostasCorretas: estatisticas.respostasCorretas,
      melhorSequencia: estatisticas.melhorSequencia
    };
  });

  enfileirarPersistencia(
    partida,
    () =>
      persistencia.finalizarPartida({
        partidaId,
        materia: partida.materia,
        totalPerguntas: partida.numeroPergunta,
        jogadores: dadosJogadores
      }),
    "finalizar partida no banco"
  );
}

/*
 * Tenta formar uma partida multiplayer.
 */
function tentarCriarPartidaMultiplayer(
  materia
) {
  const fila =
    Array.from(
      userMateria.entries()
    )
      .filter(
        ([
          ws,
          materiaEscolhida
        ]) => {
          return (
            materiaEscolhida === materia &&
            !partidasPorUsuario.has(ws) &&
            usuariosConectados.has(ws) &&
            ws.readyState === WebSocket.OPEN
          );
        }
      )
      .map(([ws]) => ws);

  if (
    fila.length < 2
  ) {
    return false;
  }

  const jogadores =
    fila.slice(0, 2);

  const partidaId =
    criarPartida(
      jogadores,
      materia
    );

  jogadores.forEach((ws) => {
    enviarJson(ws, {
      tipo: "parFormado",
      partidaId,
      materia,
      tempoAbertura: 5
    });
  });

  return true;
}

/*
 * Agenda uma partida individual.
 */
function agendarPartidaSingle(
  ws,
  materia
) {
  limparTimerSingle(ws);

  const timer =
    setTimeout(() => {
      timersSingle.delete(ws);

      if (
        ws.readyState !== WebSocket.OPEN
      ) {
        return;
      }

      if (
        partidasPorUsuario.has(ws)
      ) {
        return;
      }

      if (
        userMateria.get(ws) !== materia
      ) {
        return;
      }

      const partidaId =
        criarPartida(
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
    },
      TEMPO_ESPERA_SINGLE_MS
    );

  timersSingle.set(
    ws,
    timer
  );
}

/*
 * Envia o ranking geral ou por matéria.
 */
async function enviarRanking(ws, data) {
  const materia =
    typeof data.materia === "string" &&
      data.materia.trim() !== ""
      ? data.materia.trim().toLowerCase()
      : null;

  try {
    await aguardarPersistencias();

    const jogadores =
      await persistencia.obterRanking(
        materia,
        data.limite
      );

    enviarJson(ws, {
      tipo: "ranking",
      materia: materia || "",

      jogadores: jogadores.map(
        (jogador, indice) => ({
          posicao: indice + 1,
          usuarioId: String(jogador.id),
          nome: String(jogador.nome),
          pontos: Number(jogador.pontos),

          partidasJogadas:
            Number(jogador.partidas_jogadas),

          partidasVencidas:
            Number(jogador.partidas_vencidas)
        })
      )
    });
  } catch (erro) {
    console.error(
      "Erro ao consultar ranking:",
      erro
    );

    enviarJson(ws, {
      tipo: "erro",
      mensagem:
        "Não foi possível carregar o ranking."
    });
  }
}

/*
 * Envia as estatísticas do jogador conectado.
 */
async function enviarEstatisticas(ws) {
  const usuario = obterUsuarioConectado(ws);

  if (!usuario) {
    return;
  }

  try {
    await aguardarPersistencias();

    const estatisticas =
      await persistencia.obterEstatisticasUsuario(
        usuario.id
      );

    if (!estatisticas) {
      enviarJson(ws, {
        tipo: "erro",
        mensagem: "Usuário não encontrado no banco."
      });

      return;
    }

    enviarJson(ws, {
      tipo: "estatisticas",
      usuarioId: String(estatisticas.id),
      nome: String(estatisticas.nome),
      posicao: Number(estatisticas.posicao),
      pontos: Number(estatisticas.pontos),

      partidasJogadas:
        Number(estatisticas.partidas_jogadas),

      partidasVencidas:
        Number(estatisticas.partidas_vencidas),

      partidasPerdidas:
        Number(estatisticas.partidas_perdidas),

      partidasEmpatadas:
        Number(estatisticas.partidas_empatadas),

      respostasTotais:
        Number(estatisticas.respostas_totais),

      respostasCorretas:
        Number(estatisticas.respostas_corretas),

      sequenciaAtual:
        Number(estatisticas.sequencia_atual),

      melhorSequencia:
        Number(estatisticas.melhor_sequencia),

      materiaFavorita:
        estatisticas.materia_favorita || ""
    });
  } catch (erro) {
    console.error(
      "Erro ao consultar estatísticas:",
      erro
    );

    enviarJson(ws, {
      tipo: "erro",
      mensagem:
        "Não foi possível carregar as estatísticas."
    });
  }
}

/*
 * Conexão de um novo cliente.
 */
wss.on("connection", (ws) => {
  console.log(
    "Novo cliente conectado."
  );

  enviarJson(ws, {
    tipo: "status",
    mensagem: "Escolha a matéria..."
  });

  ws.on(
    "message",
    async (message) => {
      console.log(
        "Mensagem recebida:",
        message.toString()
      );

      try {
        const data =
          JSON.parse(
            message.toString()
          );

        /*
         * Identificação do jogador.
         */
        if (
          data.tipo ===
          "identificarUsuario"
        ) {
          await identificarUsuario(
            ws,
            data
          );

          return;
        }

        /*
         * Reinicia a sessão de partida,
         * mas preserva a identificação.
         */
        if (
          data.tipo === "reset"
        ) {
          removerJogador(ws);

          enviarJson(ws, {
            tipo: "status",
            mensagem:
              "Sessão reiniciada. " +
              "Escolha a matéria..."
          });

          return;
        }

        /*
         * Impede o uso do WebSocket sem
         * identificação prévia.
         */
        if (
          !obterUsuarioConectado(ws)
        ) {
          enviarJson(ws, {
            tipo: "erro",
            mensagem:
              "Identifique o usuário " +
              "antes de continuar."
          });

          return;
        }

        /*
         * Consulta do ranking.
         */
        if (
          data.tipo === "ranking"
        ) {
          await enviarRanking(ws, data);

          return;
        }

        /*
         * Estatísticas do próprio jogador.
         */
        if (
          data.tipo === "estatisticas"
        ) {
          await enviarEstatisticas(ws);

          return;
        }

        /*
         * Escolha da matéria.
         */
        if (
          data.materia
        ) {
          if (
            partidasPorUsuario.has(ws)
          ) {
            enviarJson(ws, {
              tipo: "status",
              mensagem:
                "Você já está em uma " +
                "partida ativa."
            });

            return;
          }

          const materia =
            String(data.materia)
              .trim()
              .toLowerCase();

          userMateria.set(
            ws,
            materia
          );

          enviarJson(ws, {
            tipo: "status",
            mensagem:
              `Você escolheu ${materia}, ` +
              "aguardando outro usuário..."
          });

          const partidaCriada =
            tentarCriarPartidaMultiplayer(
              materia
            );

          if (
            !partidaCriada
          ) {
            agendarPartidaSingle(
              ws,
              materia
            );
          }

          return;
        }

        /*
         * O Unity informa que carregou
         * a cena de perguntas.
         */
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

        /*
         * Resposta do jogador.
         */
        if (
          data.tipo === "resposta" &&
          data.partidaId
        ) {
          registrarResposta(
            ws,
            data
          );

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

        enviarJson(ws, {
          tipo: "erro",
          mensagem:
            "Mensagem inválida recebida."
        });
      }
    }
  );

  ws.on(
    "close",
    (codigo, motivo) => {
      const usuario =
        obterUsuarioConectado(ws);

      console.log(
        "Cliente desconectado:",
        usuario || "não identificado",
        "| código:",
        codigo,
        "| motivo:",
        motivo?.toString() || ""
      );

      usuariosConectados.delete(ws);

      removerJogador(ws);
    }
  );

  ws.on(
    "error",
    (erro) => {
      console.error(
        "Erro no WebSocket:",
        erro
      );

      usuariosConectados.delete(ws);
    }
  );
});

console.log(
  `Servidor WebSocket rodando na porta ${PORT}`
);