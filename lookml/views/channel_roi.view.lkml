# channel_roi.view.lkml
# View: Channel-level ROI analysis with conditional formatting

view: channel_roi {
  sql_table_name: `${project_id.value}.analytics_marketing.rpt_channel_roi` ;;

  # ---- Parameter ----

  parameter: date_range_filter {
    type: unquoted
    description: "Select date range for channel analysis"
    allowed_value: { label: "Last 30 Days"  value: "30" }
    allowed_value: { label: "Last 90 Days"  value: "90" }
    allowed_value: { label: "Last 180 Days" value: "180" }
    allowed_value: { label: "All Time"      value: "all" }
  }

  # ---- Dimensions ----

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
    description: "Marketing channel"
    primary_key: yes
    link: {
      label: "Dashboard View"
      url: "/explore/marketing-analytics/marketing_dashboard?fields=marketing_dashboard.date,marketing_dashboard.total_spend,marketing_dashboard.overall_roas&f[marketing_dashboard.channel]={{ value }}"
    }
  }

  dimension: best_campaign_name {
    type: string
    sql: ${TABLE}.best_campaign_name ;;
    description: "Best performing campaign by ROAS"
  }

  dimension: worst_campaign_name {
    type: string
    sql: ${TABLE}.worst_campaign_name ;;
    description: "Worst performing campaign by ROAS"
  }

  # ---- Measures ----

  measure: total_impressions {
    type: sum
    sql: ${TABLE}.total_impressions ;;
    description: "Total impressions across channel"
    value_format: "#,##0"
  }

  measure: total_clicks {
    type: sum
    sql: ${TABLE}.total_clicks ;;
    description: "Total clicks across channel"
    value_format: "#,##0"
  }

  measure: total_conversions {
    type: sum
    sql: ${TABLE}.total_conversions ;;
    description: "Total conversions across channel"
    value_format: "#,##0"
  }

  measure: total_spend {
    type: sum
    sql: ${TABLE}.total_spend_euros ;;
    description: "Total spend in EUR"
    value_format: "#,##0.00 \"EUR\""
  }

  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_revenue_euros ;;
    description: "Total revenue in EUR"
    value_format: "#,##0.00 \"EUR\""
  }

  measure: total_profit {
    type: sum
    sql: ${TABLE}.profit_euros ;;
    description: "Total profit in EUR"
    value_format: "#,##0.00 \"EUR\""
    html: {% if value > 0 %}
            <span style="color: #2e7d32; font-weight: bold;">{{ rendered_value }}</span>
          {% else %}
            <span style="color: #d32f2f; font-weight: bold;">{{ rendered_value }}</span>
          {% endif %} ;;
  }

  measure: avg_roi {
    type: number
    sql: AVG(${TABLE}.roi_pct) ;;
    description: "Average ROI percentage"
    value_format: "0.00\"%\""
    html: {% if value >= 100 %}
            <span style="color: #2e7d32; font-weight: bold;">{{ rendered_value }}</span>
          {% elsif value >= 0 %}
            <span style="color: #f57c00;">{{ rendered_value }}</span>
          {% else %}
            <span style="color: #d32f2f;">{{ rendered_value }}</span>
          {% endif %} ;;
  }

  measure: avg_cpa {
    type: number
    sql: AVG(${TABLE}.avg_cpa_eur) ;;
    description: "Average cost per acquisition"
    value_format: "0.00 \"EUR\""
  }

  measure: avg_roas {
    type: number
    sql: AVG(${TABLE}.avg_roas) ;;
    description: "Average return on ad spend"
    value_format: "0.00"
    html: {% if value >= 3.0 %}
            <span style="color: #2e7d32; font-weight: bold;">{{ rendered_value }}</span>
          {% elsif value >= 1.0 %}
            <span style="color: #f57c00;">{{ rendered_value }}</span>
          {% else %}
            <span style="color: #d32f2f;">{{ rendered_value }}</span>
          {% endif %} ;;
  }

  measure: avg_ctr {
    type: number
    sql: AVG(${TABLE}.ctr_pct) ;;
    description: "Average click-through rate"
    value_format: "0.00\"%\""
  }

  measure: avg_conversion_rate {
    type: number
    sql: AVG(${TABLE}.conversion_rate_pct) ;;
    description: "Average conversion rate"
    value_format: "0.00\"%\""
  }

  measure: channel_health_score {
    type: number
    sql: AVG(${TABLE}.channel_health_score) ;;
    description: "Channel health score (0-100)"
    value_format: "0"
    html: {% if value >= 70 %}
            <span style="background-color: #c8e6c9; padding: 4px 8px; border-radius: 4px; color: #2e7d32; font-weight: bold;">{{ rendered_value }}</span>
          {% elsif value >= 40 %}
            <span style="background-color: #fff9c4; padding: 4px 8px; border-radius: 4px; color: #f57c00;">{{ rendered_value }}</span>
          {% else %}
            <span style="background-color: #ffcdd2; padding: 4px 8px; border-radius: 4px; color: #d32f2f; font-weight: bold;">{{ rendered_value }}</span>
          {% endif %} ;;
  }

  measure: best_campaign_roas {
    type: max
    sql: ${TABLE}.best_campaign_roas ;;
    description: "ROAS of best campaign in channel"
    value_format: "0.00"
  }

  measure: worst_campaign_roas {
    type: min
    sql: ${TABLE}.worst_campaign_roas ;;
    description: "ROAS of worst campaign in channel"
    value_format: "0.00"
  }

  measure: channel_count {
    type: count
    description: "Number of channels"
  }
}
