from __future__ import annotations

import argparse

from .config import Stage4Config
from .spark_utils import get_spark


def build_discrepancy_backlog(spark, cfg: Stage4Config, run_id: str) -> None:
    rid = run_id.replace("'", "''")
    spark.sql(f"""
    INSERT INTO {cfg.table('discrepancias')}
    WITH latest_result AS (
      SELECT * FROM (
        SELECT r.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY processed_at DESC) AS rn
        FROM {cfg.table('resultados')} r
        WHERE run_id = '{rid}'
      ) WHERE rn = 1
    ), joined AS (
      SELECT
        r.run_id,
        r.order_id,
        r.business_decision AS agent_decision,
        ref.analyst_decision,
        r.classification_category AS agent_category,
        ref.analyst_category,
        r.confidence_score,
        r.justification,
        ref.analyst_observation,
        r.input_context_json
      FROM latest_result r
      INNER JOIN {cfg.table('analista_referencia')} ref
        ON r.order_id = ref.order_id
      WHERE COALESCE(r.business_decision, '') <> COALESCE(ref.analyst_decision, '')
         OR COALESCE(r.classification_category, '') <> COALESCE(ref.analyst_category, '')
    )
    SELECT
      run_id,
      CURRENT_TIMESTAMP() AS detected_at,
      order_id,
      agent_decision,
      analyst_decision,
      agent_category,
      analyst_category,
      confidence_score,
      justification,
      analyst_observation,
      CASE
        WHEN confidence_score >= 0.80 THEN 'revisar_regla_prompt_por_alta_confianza_errada'
        WHEN agent_decision = 'REVISION_MANUAL' THEN 'agregar_fewshot_para_reducir_revision_manual'
        ELSE 'agregar_fewshot_o_ajustar_casuistica'
      END AS suggested_action,
      TO_JSON(NAMED_STRUCT(
        'input_context_json', input_context_json,
        'expected', NAMED_STRUCT('business_decision', analyst_decision, 'classification_category', analyst_category, 'analyst_observation', analyst_observation)
      )) AS candidate_fewshot_json
    FROM joined
    """)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build discrepancy backlog for prompt correction")
    parser.add_argument("--config", default=None)
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()
    spark = get_spark()
    cfg = Stage4Config.load(args.config)
    build_discrepancy_backlog(spark, cfg, args.run_id)
    print(f"Discrepancy backlog written for run_id={args.run_id}")


if __name__ == "__main__":
    main()
