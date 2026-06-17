from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Optional

from .config import Stage4Config
from .spark_utils import get_spark


def evaluate_against_analyst(spark, cfg: Stage4Config, run_id: str, latest_only: bool = True) -> None:
    result_base = cfg.table("resultados")
    reference = cfg.table("analista_referencia")
    metrics = cfg.table("metricas")

    if latest_only:
        result_cte = f"""
        SELECT * FROM (
          SELECT r.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY processed_at DESC) AS rn
          FROM {result_base} r
          WHERE run_id = '{run_id.replace("'", "''")}'
        ) WHERE rn = 1
        """
    else:
        result_cte = f"SELECT * FROM {result_base} WHERE run_id = '{run_id.replace("'", "''")}'"

    joined_view = "stage4_eval_joined"
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW {joined_view} AS
    WITH r AS ({result_cte})
    SELECT
      r.run_id,
      r.order_id,
      r.business_decision AS agent_decision,
      ref.analyst_decision,
      r.classification_category AS agent_category,
      ref.analyst_category,
      r.confidence_score,
      r.requires_human_review,
      r.technical_status
    FROM r
    INNER JOIN {reference} ref
      ON r.order_id = ref.order_id
    """)

    now_expr = "CURRENT_TIMESTAMP()"
    spark.sql(f"""
    INSERT INTO {metrics}
    SELECT '{run_id}' AS run_id, {now_expr} AS evaluated_at, 'accuracy_decision' AS metric_name,
           AVG(CASE WHEN agent_decision = analyst_decision THEN 1.0 ELSE 0.0 END) AS metric_value,
           TO_JSON(NAMED_STRUCT('n', COUNT(*))) AS metric_detail
    FROM {joined_view}
    """)

    spark.sql(f"""
    INSERT INTO {metrics}
    SELECT '{run_id}' AS run_id, {now_expr} AS evaluated_at, 'coverage_auto_decision' AS metric_name,
           AVG(CASE WHEN requires_human_review = false THEN 1.0 ELSE 0.0 END) AS metric_value,
           TO_JSON(NAMED_STRUCT('n', COUNT(*))) AS metric_detail
    FROM {joined_view}
    """)

    spark.sql(f"""
    INSERT INTO {metrics}
    SELECT '{run_id}' AS run_id, {now_expr} AS evaluated_at, 'accuracy_category' AS metric_name,
           AVG(CASE WHEN agent_category = analyst_category THEN 1.0 ELSE 0.0 END) AS metric_value,
           TO_JSON(NAMED_STRUCT('n', COUNT(*))) AS metric_detail
    FROM {joined_view}
    WHERE analyst_category IS NOT NULL
    """)

    spark.sql(f"""
    INSERT INTO {metrics}
    SELECT
      '{run_id}' AS run_id,
      {now_expr} AS evaluated_at,
      'confusion_matrix_decision' AS metric_name,
      CAST(COUNT(*) AS DOUBLE) AS metric_value,
      TO_JSON(NAMED_STRUCT('agent_decision', agent_decision, 'analyst_decision', analyst_decision, 'n', COUNT(*))) AS metric_detail
    FROM {joined_view}
    GROUP BY agent_decision, analyst_decision
    """)


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate MIDAS Stage 4 agent vs analyst labels")
    parser.add_argument("--config", default=None)
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()
    spark = get_spark()
    cfg = Stage4Config.load(args.config)
    evaluate_against_analyst(spark, cfg, run_id=args.run_id)
    print(f"Evaluation metrics written for run_id={args.run_id}")


if __name__ == "__main__":
    main()
