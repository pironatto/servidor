"use strict";

const K_FACTOR = 24;
const RATING_INICIAL = 1000;
const RATING_MINIMO = 0;

/*
 * Calcula a probabilidade esperada de vitória
 * do jogador em relação ao adversário.
 */
function calcularRatingEsperado(
    ratingJogador,
    ratingOponente
) {
    const jogador =
        normalizarRating(ratingJogador);

    const oponente =
        normalizarRating(ratingOponente);

    return 1 / (
        1 +
        Math.pow(
            10,
            (oponente - jogador) / 400
        )
    );
}

/*
 * Calcula a variação Elo de um jogador.
 *
 * resultado:
 * - vitoria
 * - derrota
 * - empate
 */
function calcularVariacaoElo(
    ratingJogador,
    ratingOponente,
    resultado
) {
    const ratingAtual =
        normalizarRating(ratingJogador);

    const ratingAdversario =
        normalizarRating(ratingOponente);

    const ratingEsperado =
        calcularRatingEsperado(
            ratingAtual,
            ratingAdversario
        );

    const resultadoNormalizado =
        normalizarResultado(resultado);

    let resultadoReal;

    switch (resultadoNormalizado) {
        case "vitoria":
            resultadoReal = 1;
            break;

        case "empate":
            resultadoReal = 0.5;
            break;

        case "derrota":
            resultadoReal = 0;
            break;

        default:
            throw new Error(
                `Resultado inválido para Elo: ${resultado}`
            );
    }

    const variacaoRating =
        Math.round(
            K_FACTOR *
            (resultadoReal - ratingEsperado)
        );

    const ratingNovo =
        Math.max(
            RATING_MINIMO,
            ratingAtual + variacaoRating
        );

    return {
        ratingAnterior: ratingAtual,
        ratingEsperado,
        resultadoReal,
        variacaoRating,
        ratingNovo
    };
}

/*
 * Determina o resultado com base
 * na pontuação dos jogadores.
 */
function calcularResultado(
    pontuacaoJogador,
    pontuacaoOponente
) {
    const jogador =
        Number(pontuacaoJogador) || 0;

    const oponente =
        Number(pontuacaoOponente) || 0;

    if (jogador > oponente) {
        return "vitoria";
    }

    if (jogador < oponente) {
        return "derrota";
    }

    return "empate";
}

/*
 * Obtém o rating de um usuário a partir
 * de um Map de ratings.
 */
function obterRating(
    ratings,
    usuarioId
) {
    if (
        !ratings ||
        typeof ratings.get !== "function"
    ) {
        return RATING_INICIAL;
    }

    const rating =
        ratings.get(String(usuarioId));

    return normalizarRating(rating);
}

/*
 * Normaliza um rating recebido do banco
 * ou de outra fonte.
 */
function normalizarRating(rating) {
    const valor =
        Number(rating);

    if (
        !Number.isFinite(valor)
    ) {
        return RATING_INICIAL;
    }

    return Math.max(
        RATING_MINIMO,
        Math.round(valor)
    );
}

/*
 * Garante que o resultado esteja
 * em um dos valores aceitos.
 */
function normalizarResultado(resultado) {
    if (
        typeof resultado !== "string"
    ) {
        return "";
    }

    return resultado
        .trim()
        .toLowerCase();
}

/* ============================================================
 * Faixas de rating (espelhado do PHP ranking.php)
 * ============================================================
 *
 * Traduz um rating numérico em uma faixa visual.
 * Mantenha em sincronia com obterFaixa() do ranking.php.
 */
function obterFaixa(rating) {
    const r = Number(rating) || 0;

    // 🥉 Bronze — Aprendiz
    if (r < 900) return { nome: "Aprendiz III", cor: "#8B5A2B", emoji: "🥉" };
    if (r < 1000) return { nome: "Aprendiz II", cor: "#A0522D", emoji: "🥉" };
    if (r < 1100) return { nome: "Aprendiz I", cor: "#CD7F32", emoji: "🥉" };

    // 🥈 Prata — Estudante
    if (r < 1200) return { nome: "Estudante III", cor: "#B0B0B0", emoji: "🥈" };
    if (r < 1300) return { nome: "Estudante II", cor: "#C0C0C0", emoji: "🥈" };
    if (r < 1400) return { nome: "Estudante I", cor: "#D3D3D3", emoji: "🥈" };

    // 🥇 Ouro — Monitor
    if (r < 1500) return { nome: "Monitor III", cor: "#DAA520", emoji: "🥇" };
    if (r < 1600) return { nome: "Monitor II", cor: "#FFD700", emoji: "🥇" };
    if (r < 1700) return { nome: "Monitor I", cor: "#FFA500", emoji: "🥇" };

    // 💠 Faixas altas
    if (r < 1900) return { nome: "Professor", cor: "#5F9EA0", emoji: "💠" };
    if (r < 2100) return { nome: "Mestre", cor: "#8A2BE2", emoji: "💎" };

    return { nome: "Sábio", cor: "#DC143C", emoji: "👑" };
}




module.exports = {
    K_FACTOR,
    RATING_INICIAL,
    RATING_MINIMO,
    calcularRatingEsperado,
    calcularVariacaoElo,
    calcularResultado,
    obterRating,
    normalizarRating,
    normalizarResultado,
    obterFaixa  // 🆕
};