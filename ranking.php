<?php

/**
 * ranking.php
 * API HTTP chamada pelo Unity para ranking geral, ranking por matéria
 * e histórico do jogador.
 *
 * Ações:
 *   ?acao=geral                 -> ranking geral
 *   ?acao=materia (POST materia)-> ranking por matéria
 *   ?acao=historico (POST usuarioId) -> histórico do jogador
 */

include_once('conexao.php');

header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

/* Responde a requisições OPTIONS (preflight) */
if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(204);
    exit;
}

$acao      = $_GET['acao']       ?? '';
$usuarioId = $_POST['usuarioId'] ?? '';
$materia   = $_POST['materia']   ?? '';

switch ($acao) {
    case 'geral':
        obterRankingGeral();
        break;

    case 'materia':
        obterRankingMateria($materia);
        break;

    case 'historico':
        obterHistoricoUsuario($usuarioId);
        break;

    default:
        http_response_code(400);
        echo json_encode(['erro' => 'Ação não reconhecida']);
        break;
}

/* =============================================================
 * Ranking geral
 * ============================================================= */
function obterRankingGeral()
{
    global $db;

    try {
        $sql = $db->prepare(
            "SELECT
                u.id,
                u.nome,
                COALESCE(u.rating, 1000)          AS rating,
                COALESCE(u.melhor_rating, 1000)   AS melhor_rating,
                COALESCE(u.ultima_variacao_rating, 0) AS ultima_variacao_rating,
                COALESCE(u.partidas_rating, 0)    AS partidas_rating,
                COALESCE(u.pontos, 0)             AS pontos,
                u.partidas_jogadas,
                u.partidas_vencidas,
                u.partidas_perdidas,
                u.partidas_empatadas
             FROM usuarios u
             WHERE u.partidas_jogadas > 0
             ORDER BY rating DESC, pontos DESC, partidas_vencidas DESC
             LIMIT 20"
        );

        $sql->execute();
        $ranking = $sql->fetchAll(PDO::FETCH_ASSOC);

        echo json_encode([
            'tipo'  => 'rankingGeral',
            'dados' => array_map('normalizarLinhaRanking', $ranking)
        ]);
    } catch (PDOException $erro) {
        http_response_code(500);
        echo json_encode(['erro' => $erro->getMessage()]);
    }
}

/* =============================================================
 * Ranking por matéria
 * ============================================================= */
function obterRankingMateria($materia)
{
    global $db;

    if (empty($materia)) {
        http_response_code(400);
        echo json_encode(['erro' => 'Matéria não informada']);
        return;
    }

    try {
        $sql = $db->prepare(
            "SELECT
                u.id,
                u.nome,
                um.materia,
                COALESCE(um.rating, 1000)          AS rating,
                COALESCE(um.melhor_rating, 1000)   AS melhor_rating,
                COALESCE(um.ultima_variacao_rating, 0) AS ultima_variacao_rating,
                COALESCE(um.partidas_rating, 0)    AS partidas_rating,
                COALESCE(um.pontos, 0)             AS pontos,
                um.partidas_jogadas,
                um.partidas_vencidas,
                um.partidas_perdidas,
                um.partidas_empatadas
             FROM usuario_materias um
             JOIN usuarios u ON u.id = um.usuario_id
             WHERE um.materia = :materia
               AND um.partidas_jogadas > 0
             ORDER BY rating DESC, pontos DESC, um.partidas_vencidas DESC
             LIMIT 20"
        );

        $sql->bindParam(':materia', $materia, PDO::PARAM_STR);
        $sql->execute();
        $ranking = $sql->fetchAll(PDO::FETCH_ASSOC);

        echo json_encode([
            'tipo'    => 'rankingMateria',
            'materia' => $materia,
            'dados'   => array_map('normalizarLinhaRankingMateria', $ranking)
        ]);
    } catch (PDOException $erro) {
        http_response_code(500);
        echo json_encode(['erro' => $erro->getMessage()]);
    }
}

/* =============================================================
 * Histórico do usuário
 * ============================================================= */
function obterHistoricoUsuario($usuarioId)
{
    global $db;

    if (empty($usuarioId)) {
        http_response_code(400);
        echo json_encode(['erro' => 'Usuário não informado']);
        return;
    }

    try {
        $sql = $db->prepare(
            "SELECT
                pj.partida_id,
                p.materia,
                pj.resultado,
                COALESCE(pj.pontuacao, 0)          AS pontuacao,
                pj.respostas_totais,
                pj.respostas_corretas,
                COALESCE(pj.rating_anterior, 0)    AS rating_anterior,
                COALESCE(pj.rating_novo, 0)        AS rating_novo,
                COALESCE(pj.variacao_rating, 0)    AS variacao_rating,
                pj.saiu_em
             FROM partida_jogadores pj
             JOIN partidas p ON p.id = pj.partida_id
             WHERE pj.usuario_id = :usuarioId
               AND p.status = 'finalizada'
               AND pj.resultado IS NOT NULL
             ORDER BY pj.saiu_em DESC
             LIMIT 30"
        );

        $sql->bindParam(':usuarioId', $usuarioId, PDO::PARAM_STR);
        $sql->execute();
        $historico = $sql->fetchAll(PDO::FETCH_ASSOC);

        echo json_encode([
            'tipo'      => 'historicoUsuario',
            'usuarioId' => $usuarioId,
            'dados'     => array_map('normalizarLinhaHistorico', $historico)
        ]);
    } catch (PDOException $erro) {
        http_response_code(500);
        echo json_encode(['erro' => $erro->getMessage()]);
    }
}

/* =============================================================
 * Normalizadores (MySQL devolve DECIMAL como string, e NULL como null)
 * ============================================================= */

function normalizarLinhaRanking(array $linha): array
{
    return [
        'id'                     => (string) $linha['id'],
        'nome'                   => (string) $linha['nome'],
        'rating'                 => (int)    $linha['rating'],
        'melhor_rating'          => (int)    $linha['melhor_rating'],
        'ultima_variacao_rating' => (int)    $linha['ultima_variacao_rating'],
        'partidas_rating'        => (int)    $linha['partidas_rating'],
        'pontos'                 => (float)  $linha['pontos'],
        'partidas_jogadas'       => (int)    $linha['partidas_jogadas'],
        'partidas_vencidas'      => (int)    $linha['partidas_vencidas'],
        'partidas_perdidas'      => (int)    $linha['partidas_perdidas'],
        'partidas_empatadas'     => (int)    $linha['partidas_empatadas']
    ];
}

function normalizarLinhaRankingMateria(array $linha): array
{
    return [
        'id'                     => (string) $linha['id'],
        'nome'                   => (string) $linha['nome'],
        'materia'                => (string) $linha['materia'],
        'rating'                 => (int)    $linha['rating'],
        'melhor_rating'          => (int)    $linha['melhor_rating'],
        'ultima_variacao_rating' => (int)    $linha['ultima_variacao_rating'],
        'partidas_rating'        => (int)    $linha['partidas_rating'],
        'pontos'                 => (float)  $linha['pontos'],
        'partidas_jogadas'       => (int)    $linha['partidas_jogadas'],
        'partidas_vencidas'      => (int)    $linha['partidas_vencidas'],
        'partidas_perdidas'      => (int)    $linha['partidas_perdidas'],
        'partidas_empatadas'     => (int)    $linha['partidas_empatadas']
    ];
}

function normalizarLinhaHistorico(array $linha): array
{
    return [
        'partida_id'         => (string) $linha['partida_id'],
        'materia'            => (string) $linha['materia'],
        'resultado'          => (string) ($linha['resultado'] ?? ''),
        'pontuacao'          => (float)  $linha['pontuacao'],
        'respostas_totais'   => (int)    $linha['respostas_totais'],
        'respostas_corretas' => (int)    $linha['respostas_corretas'],
        'rating_anterior'    => (int)    $linha['rating_anterior'],
        'rating_novo'        => (int)    $linha['rating_novo'],
        'variacao_rating'    => (int)    $linha['variacao_rating'],
        'saiu_em'            => (string) ($linha['saiu_em'] ?? '')
    ];
}
