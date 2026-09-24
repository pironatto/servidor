const mysql = require("mysql2/promise");

const db = mysql.createPool({
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "fantomas",
  database: process.env.DB_NAME || "quiz",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

/*
 * Cria a partida e os jogadores no banco.
 *
 * jogadores: [{ id }]
 */
async function registrarPartida(
  partidaId,
  materia,
  modo,
  jogadores
) {
  const conexao = await db.getConnection();

  try {
    await conexao.beginTransaction();

    await conexao.execute(
      `
      INSERT INTO partidas
        (id, materia, modo, status)
      VALUES
        (?, ?, ?, 'em_andamento')
      `,
      [partidaId, materia, modo]
    );

    for (const jogador of jogadores) {
      await conexao.execute(
        `
        INSERT INTO partida_jogadores
          (partida_id, usuario_id)
        VALUES
          (?, ?)
        `,
        [partidaId, jogador.id]
      );
    }

    await conexao.commit();
  } catch (erro) {
    await conexao.rollback();
    throw erro;
  } finally {
    conexao.release();
  }
}

/*
 * Grava a resposta de um jogador e atualiza os
 * contadores de acertos do usuário.
 */
async function registrarResposta(dados) {
  const acertou = dados.acertou ? 1 : 0;

  const conexao = await db.getConnection();

  try {
    await conexao.beginTransaction();

    await conexao.execute(
      `
      INSERT INTO partida_respostas
        (partida_id,
         usuario_id,
         pergunta_id,
         resposta,
         resposta_correta,
         acertou,
         pontos,
         tempo_resposta_ms)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        resposta = VALUES(resposta),
        acertou = VALUES(acertou),
        pontos = VALUES(pontos),
        tempo_resposta_ms = VALUES(tempo_resposta_ms)
      `,
      [
        dados.partidaId,
        dados.usuarioId,
        dados.perguntaId,
        dados.resposta || null,
        dados.respostaCorreta,
        acertou,
        dados.pontos,
        dados.tempoRespostaMs
      ]
    );

    await conexao.execute(
      `
      UPDATE usuarios
      SET
        respostas_totais = respostas_totais + 1,
        respostas_corretas = respostas_corretas + ?,
        melhor_sequencia = GREATEST(
          melhor_sequencia,
          IF(?, sequencia_atual + 1, 0)
        ),
        sequencia_atual = IF(?, sequencia_atual + 1, 0)
      WHERE id = ?
      `,
      [
        acertou,
        acertou,
        acertou,
        dados.usuarioId
      ]
    );

    await conexao.commit();
  } catch (erro) {
    await conexao.rollback();
    throw erro;
  } finally {
    conexao.release();
  }
}

/*
 * Fecha a partida e consolida as estatísticas.
 *
 * jogadores: [{
 *   id,
 *   pontuacao,
 *   resultado,
 *   respostasTotais,
 *   respostasCorretas,
 *   melhorSequencia
 * }]
 */
async function finalizarPartida(dados) {
  const conexao = await db.getConnection();

  try {
    await conexao.beginTransaction();

    await conexao.execute(
      `
      UPDATE partidas
      SET
        status = 'finalizada',
        total_perguntas = ?,
        finalizada_em = NOW()
      WHERE id = ?
      `,
      [
        dados.totalPerguntas,
        dados.partidaId
      ]
    );

    for (const jogador of dados.jogadores) {
      await conexao.execute(
        `
        UPDATE partida_jogadores
        SET
          pontuacao = ?,
          resultado = ?,
          respostas_totais = ?,
          respostas_corretas = ?,
          melhor_sequencia = ?,
          saiu_em = NOW()
        WHERE partida_id = ?
          AND usuario_id = ?
        `,
        [
          jogador.pontuacao,
          jogador.resultado,
          jogador.respostasTotais,
          jogador.respostasCorretas,
          jogador.melhorSequencia,
          dados.partidaId,
          jogador.id
        ]
      );

      await conexao.execute(
        `
        UPDATE usuarios
        SET
          pontos = pontos + ?,
          partidas_jogadas = partidas_jogadas + 1,
          partidas_vencidas = partidas_vencidas + ?,
          partidas_perdidas = partidas_perdidas + ?,
          partidas_empatadas = partidas_empatadas + ?
        WHERE id = ?
        `,
        [
          jogador.pontuacao,
          jogador.resultado === "vitoria" ? 1 : 0,
          jogador.resultado === "derrota" ? 1 : 0,
          jogador.resultado === "empate" ? 1 : 0,
          jogador.id
        ]
      );

      await conexao.execute(
        `
        INSERT INTO usuario_materias
          (usuario_id,
           materia,
           partidas_jogadas,
           partidas_vencidas,
           partidas_perdidas,
           partidas_empatadas,
           pontos,
           respostas_totais,
           respostas_corretas)
        VALUES
          (?, ?, 1, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          partidas_jogadas = partidas_jogadas + 1,
          partidas_vencidas =
            partidas_vencidas + VALUES(partidas_vencidas),
          partidas_perdidas =
            partidas_perdidas + VALUES(partidas_perdidas),
          partidas_empatadas =
            partidas_empatadas + VALUES(partidas_empatadas),
          pontos = pontos + VALUES(pontos),
          respostas_totais =
            respostas_totais + VALUES(respostas_totais),
          respostas_corretas =
            respostas_corretas + VALUES(respostas_corretas)
        `,
        [
          jogador.id,
          dados.materia,
          jogador.resultado === "vitoria" ? 1 : 0,
          jogador.resultado === "derrota" ? 1 : 0,
          jogador.resultado === "empate" ? 1 : 0,
          jogador.pontuacao,
          jogador.respostasTotais,
          jogador.respostasCorretas,
        ]
      );

      await conexao.execute(
        `
        UPDATE usuarios
        SET materia_favorita = (
          SELECT materia
          FROM usuario_materias
          WHERE usuario_id = ?
          ORDER BY
            partidas_jogadas DESC,
            pontos DESC
          LIMIT 1
        )
        WHERE id = ?
        `,
        [jogador.id, jogador.id]
      );
    }

    await conexao.commit();
  } catch (erro) {
    await conexao.rollback();
    throw erro;
  } finally {
    conexao.release();
  }
}

/*
 * Marca a partida como cancelada.
 */
async function cancelarPartida(partidaId) {
  await db.execute(
    `
    UPDATE partidas
    SET
      status = 'cancelada',
      finalizada_em = NOW()
    WHERE id = ?
      AND status = 'em_andamento'
    `,
    [partidaId]
  );

  await db.execute(
    `
    UPDATE partida_jogadores
    SET
      resultado = COALESCE(resultado, 'abandono'),
      saiu_em = COALESCE(saiu_em, NOW())
    WHERE partida_id = ?
    `,
    [partidaId]
  );
}

/*
 * Ranking geral ou por matéria.
 */
async function obterRanking(materia, limite) {
  const total = Math.min(
    Math.max(Number(limite) || 10, 1),
    50
  );

  if (materia) {
    const [rows] = await db.query(
      `
      SELECT
        u.id,
        u.nome,
        um.pontos,
        um.partidas_jogadas,
        um.partidas_vencidas
      FROM usuario_materias um
      INNER JOIN usuarios u
        ON u.id = um.usuario_id
      WHERE um.materia = ?
      ORDER BY
        um.pontos DESC,
        um.partidas_vencidas DESC
      LIMIT ?
      `,
      [materia, total]
    );

    return rows;
  }

  const [rows] = await db.query(
    `
    SELECT
      id,
      nome,
      pontos,
      partidas_jogadas,
      partidas_vencidas
    FROM usuarios
    ORDER BY
      pontos DESC,
      partidas_vencidas DESC
    LIMIT ?
    `,
    [total]
  );

  return rows;
}

/*
 * Posição e estatísticas de um usuário.
 */
async function obterEstatisticasUsuario(usuarioId) {
  const [rows] = await db.execute(
    `
    SELECT
      id,
      nome,
      pontos,
      partidas_jogadas,
      partidas_vencidas,
      partidas_perdidas,
      partidas_empatadas,
      respostas_totais,
      respostas_corretas,
      sequencia_atual,
      melhor_sequencia,
      materia_favorita
    FROM usuarios
    WHERE id = ?
    LIMIT 1
    `,
    [usuarioId]
  );

  if (!rows || rows.length === 0) {
    return null;
  }

  const [posicao] = await db.execute(
    `
    SELECT COUNT(*) + 1 AS posicao
    FROM usuarios
    WHERE pontos > (
      SELECT pontos
      FROM usuarios
      WHERE id = ?
    )
    `,
    [usuarioId]
  );

  return {
    ...rows[0],
    posicao: Number(posicao[0].posicao)
  };
}

module.exports = {
  db,
  registrarPartida,
  registrarResposta,
  finalizarPartida,
  cancelarPartida,
  obterRanking,
  obterEstatisticasUsuario
};
