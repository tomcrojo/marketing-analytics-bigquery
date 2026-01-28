# marketing_dashboard.view.lkml
# View: Daily marketing performance for dashboard tiles

view: marketing_dashboard {
  sql_table_name: `${project_id.value}.analytics_marketing.rpt_marketing_dashboard` ;;

  # ---- Dimensions ----

  dimension: date {
    type: date
    sql: ${TABLE}.date ;;
    description: "Date of the marketing activity"
    convert_tz: no
  }

  dimension: week {
    type: date_week
    sql: ${TABLE}.date ;;
    description: "Week of the marketing activity"
  }

  dimension: month {
    type: date_month
    sql: ${TABLE}.date ;;
    description: "Month of the marketing activity"
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
    description: "Marketing channel (Google_Ads, Meta_Ads, TikTok_Ads, LinkedIn_Ads, Email, SEO)"
    link: {
      label: "Channel ROI Details"
      url: "/explore/marketing-analytics/channel_roi?fields=channel_roi.channel,channel_roi.total_spend_euros,channel_roi.avg_roas&f[channel_roi.channel]={{ value }}"
    }
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
    description: "Platform identifier (google, meta, tiktok, linkedin, email, organic)"
  }

  # ---- Measures ----

  measure: total_impressions {
    type: sum
    sql: ${TABLE}.impressions ;;
    description: "Total ad impressions served"
    value_format: "#,##0"
  }

  measure: total_clicks {
    type: sum
    sql: ${TABLE}.clicks ;;
    description: "Total ad clicks"
    value_format: "#,##0"
  }

  measure: total_conversions {
    type: sum
    sql: ${TABLE}.conversions ;;
    description: "Total conversions (purchases + signups + leads)"
    value_format: "#,##0"
  }

  measure: total_spend {
    type: sum
    sql: ${TABLE}.spend_euros ;;
    description: "Total spend in EUR"
    value_format: "#,##0.00 \"EUR\""
    html: <span style="color: #d32f2f;">{{ rendered_value }}</span> ;;
  }

  measure: total_revenue {
    type: sum
    sql: ${TABLE}.revenue_euros ;;
    description: "Total revenue in EUR"
    value_format: "#,##0.00 \"EUR\""
    html: <span style="color: #2e7d32;">{{ rendered_value }}</span> ;;
  }

  measure: total_profit {
    type: number
    sql: ${total_revenue} - ${total_spend} ;;
    description: "Total profit (revenue minus spend)"
    value_format: "#,##0.00 \"EUR\""
    html: {% if value > 0 %}
            <span style="color: #2e7d32; font-weight: bold;">{{ rendered_value }}</span>
          {% else %}
            <span style="color: #d32f2f; font-weight: bold;">{{ rendered_value }}</span>
          {% endif %} ;;
  }

  measure: avg_ctr {
    type: number
    sql: SAFE_DIVIDE(${total_clicks}, ${total_impressions}) * 100 ;;
    description: "Average click-through rate (%)"
    value_format: "0.00\"%\""
  }

  measure: avg_cpc {
    type: number
    sql: SAFE_DIVIDE(${total_spend}, ${total_clicks}) ;;
    description: "Average cost per click (EUR)"
    value_format: "0.00 \"EUR\""
  }

  measure: avg_cpa {
    type: number
    sql: SAFE_DIVIDE(${total_spend}, ${total_conversions}) ;;
    description: "Average cost per acquisition (EUR)"
    value_format: "0.00 \"EUR\""
  }

  measure: overall_roas {
    type: number
    sql: SAFE_DIVIDE(${total_revenue}, ${total_spend}) ;;
    description: "Overall return on ad spend"
    value_format: "0.00"
    html: {% if value >= 3.0 %}
            <span style="color: #2e7d32; font-weight: bold;">{{ rendered_value }}</span>
          {% elsif value >= 1.0 %}
            <span style="color: #f57c00;">{{ rendered_value }}</span>
          {% else %}
            <span style="color: #d32f2f;">{{ rendered_value }}</span>
          {% endif %} ;;
  }

  measure: avg_conversion_rate {
    type: number
    sql: SAFE_DIVIDE(${total_conversions}, ${total_clicks}) * 100 ;;
    description: "Average conversion rate (%)"
    value_format: "0.00\"%\""
  }

  measure: total_purchases {
    type: sum
    sql: ${TABLE}.purchases ;;
    description: "Total purchase conversions"
    value_format: "#,##0"
  }

  measure: total_signups {
    type: sum
    sql: ${TABLE}.signups ;;
    description: "Total signup conversions"
    value_format: "#,##0"
  }

  measure: total_leads {
    type: sum
    sql: ${TABLE}.leads ;;
    description: "Total lead conversions"
    value_format: "#,##0"
  }

  measure: count_days {
    type: count_distinct
    sql: ${TABLE}.date ;;
    description: "Number of days with data"
  }

  # ---- Week-over-week derived table ----

  derived_table: {
    explore_source: marketing_dashboard {
      column: date { field: marketing_dashboard.date }
      column: channel { field: marketing_dashboard.channel }
      column: spend { field: marketing_dashboard.total_spend }
      column: revenue { field: marketing_dashboard.total_revenue }
      column: conversions { field: marketing_dashboard.total_conversions }
      column: impressions { field: marketing_dashboard.total_impressions }
    }
  }
}
