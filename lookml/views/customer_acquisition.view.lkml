# customer_acquisition.view.lkml
# View: Customer acquisition funnel with LTV, CAC, and health indicators

view: customer_acquisition {
  sql_table_name: `${project_id.value}.analytics_marketing.rpt_customer_acquisition` ;;

  # ---- Dimensions ----

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
    description: "Acquisition channel"
    primary_key: yes
  }

  dimension: acquisition_month {
    type: date_month
    sql: ${TABLE}.acquisition_month ;;
    description: "Month of customer acquisition"
    convert_tz: no
  }

  dimension: acquisition_health {
    type: string
    sql: ${TABLE}.acquisition_health ;;
    description: "Acquisition health indicator: healthy (LTV/CAC >= 3), marginal (>= 1), unhealthy (< 1)"
    html: {% if value == 'healthy' %}
            <span style="background-color: #c8e6c9; padding: 4px 12px; border-radius: 4px; color: #2e7d32; font-weight: bold;">{{ value }}</span>
          {% elsif value == 'marginal' %}
            <span style="background-color: #fff9c4; padding: 4px 12px; border-radius: 4px; color: #f57c00;">{{ value }}</span>
          {% else %}
            <span style="background-color: #ffcdd2; padding: 4px 12px; border-radius: 4px; color: #d32f2f; font-weight: bold;">{{ value }}</span>
          {% endif %} ;;
  }

  # ---- Measures ----

  measure: total_new_customers {
    type: sum
    sql: ${TABLE}.new_customers ;;
    description: "Total new customers acquired"
    value_format: "#,##0"
  }

  measure: avg_cac {
    type: number
    sql: SAFE_DIVIDE(SUM(${TABLE}.cac_eur * ${TABLE}.new_customers), SUM(${TABLE}.new_customers)) ;;
    description: "Weighted average customer acquisition cost"
    value_format: "0.00 \"EUR\""
  }

  measure: avg_first_order_value {
    type: number
    sql: AVG(${TABLE}.avg_first_order_value) ;;
    description: "Average first order value"
    value_format: "0.00 \"EUR\""
  }

  measure: avg_ltv_30d {
    type: number
    sql: AVG(${TABLE}.avg_ltv_30d) ;;
    description: "Average 30-day LTV"
    value_format: "0.00 \"EUR\""
  }

  measure: avg_ltv_90d {
    type: number
    sql: AVG(${TABLE}.avg_ltv_90d) ;;
    description: "Average 90-day LTV"
    value_format: "0.00 \"EUR\""
  }

  measure: avg_payback_period {
    type: number
    sql: AVG(${TABLE}.payback_period_days) ;;
    description: "Average payback period in days"
    value_format: "0"
    html: {% if value <= 30 %}
            <span style="color: #2e7d32; font-weight: bold;">{{ rendered_value }} days</span>
          {% elsif value <= 90 %}
            <span style="color: #f57c00;">{{ rendered_value }} days</span>
          {% else %}
            <span style="color: #d32f2f; font-weight: bold;">{{ rendered_value }} days</span>
          {% endif %} ;;
  }

  measure: avg_ltv_cac_ratio {
    type: number
    sql: AVG(${TABLE}.ltv_90d_to_cac_ratio) ;;
    description: "Average 90-day LTV to CAC ratio"
    value_format: "0.00"
    html: {% if value >= 3.0 %}
            <span style="color: #2e7d32; font-weight: bold;">{{ rendered_value }}x</span>
          {% elsif value >= 1.0 %}
            <span style="color: #f57c00;">{{ rendered_value }}x</span>
          {% else %}
            <span style="color: #d32f2f; font-weight: bold;">{{ rendered_value }}x</span>
          {% endif %} ;;
  }

  measure: avg_conversions_per_customer {
    type: number
    sql: AVG(${TABLE}.avg_conversions_per_customer) ;;
    description: "Average conversions per customer"
    value_format: "0.0"
  }

  measure: channel_count {
    type: count
    description: "Number of channel-month combinations"
  }
}
