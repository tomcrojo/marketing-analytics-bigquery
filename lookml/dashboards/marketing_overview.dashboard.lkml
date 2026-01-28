- dashboard: marketing_overview
  title: "Marketing Performance Overview"
  layout: newspaper
  preferred_viewer: dashboards-next
  crossfilter_enabled: true
  description: "Executive dashboard for multi-channel marketing performance analysis across Google Ads, Meta Ads, TikTok Ads, LinkedIn Ads, Email, and SEO"

  filters:

  - name: date_range
    title: "Date Range"
    type: date_filter
    default_value: "last 90 days"
    allow_multiple_values: false
    ui_config:
      type: relative_timeframes

  - name: channel_filter
    title: "Channel"
    type: field_filter
    explore: marketing_dashboard
    field: marketing_dashboard.channel
    default_value: ""
    allow_multiple_values: true
    ui_config:
      type: checkboxes

  - name: platform_filter
    title: "Platform"
    type: field_filter
    explore: marketing_dashboard
    field: marketing_dashboard.platform
    default_value: ""
    allow_multiple_values: true
    ui_config:
      type: checkboxes

  elements:

  # ---- Row 1: KPI Summary Cards ----

  - name: kpi_total_spend
    title: "Total Spend"
    type: single_value
    explore: marketing_dashboard
    type_config:
      comparison: {}
    measures: [marketing_dashboard.total_spend]
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    row: 0
    col: 0
    width: 4
    height: 3

  - name: kpi_total_revenue
    title: "Total Revenue"
    type: single_value
    explore: marketing_dashboard
    type_config:
      comparison: {}
    measures: [marketing_dashboard.total_revenue]
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    row: 0
    col: 4
    width: 4
    height: 3

  - name: kpi_overall_roas
    title: "Overall ROAS"
    type: single_value
    explore: marketing_dashboard
    type_config:
      comparison: {}
    measures: [marketing_dashboard.overall_roas]
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    row: 0
    col: 8
    width: 4
    height: 3

  - name: kpi_total_conversions
    title: "Total Conversions"
    type: single_value
    explore: marketing_dashboard
    type_config:
      comparison: {}
    measures: [marketing_dashboard.total_conversions]
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    row: 0
    col: 12
    width: 4
    height: 3

  - name: kpi_avg_ctr
    title: "Avg CTR"
    type: single_value
    explore: marketing_dashboard
    type_config:
      comparison: {}
    measures: [marketing_dashboard.avg_ctr]
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    row: 0
    col: 16
    width: 4
    height: 3

  - name: kpi_total_profit
    title: "Total Profit"
    type: single_value
    explore: marketing_dashboard
    type_config:
      comparison: {}
    measures: [marketing_dashboard.total_profit]
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    row: 0
    col: 20
    width: 4
    height: 3

  # ---- Row 2: Spend by Channel + ROAS Trend ----

  - name: spend_by_channel
    title: "Spend by Channel (EUR)"
    type: looker_bar
    explore: marketing_dashboard
    dimensions: [marketing_dashboard.channel]
    measures: [marketing_dashboard.total_spend]
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    sorts: [marketing_dashboard.total_spend desc]
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    show_x_axis_label: true
    show_x_axis_ticks: true
    colors: ["#1a73e8"]
    label_density: 25
    legend_position: center
    row: 3
    col: 0
    width: 12
    height: 8

  - name: roas_trend
    title: "ROAS Trend Over Time"
    type: looker_line
    explore: marketing_dashboard
    dimensions: [marketing_dashboard.date]
    measures: [marketing_dashboard.overall_roas]
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    sorts: [marketing_dashboard.date]
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    colors: ["#34a853"]
    point_style: circle
    interpolation: monotone
    y_axes: [{label: "ROAS", orientation: left}]
    reference_lines: [{reference_type: line, line_value: "1.0", label: "Breakeven", color: "#ea4335"}]
    row: 3
    col: 12
    width: 12
    height: 8

  # ---- Row 3: Funnel + Channel ROI Table ----

  - name: conversion_funnel
    title: "Conversion Funnel"
    type: looker_bar
    explore: marketing_dashboard
    measures:
      - marketing_dashboard.total_impressions
      - marketing_dashboard.total_clicks
      - marketing_dashboard.total_conversions
    listen:
      date_range: marketing_dashboard.date
      channel_filter: marketing_dashboard.channel
      platform_filter: marketing_dashboard.platform
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    colors: ["#4285f4", "#fbbc04", "#34a853"]
    series_colors:
      impressions: "#4285f4"
      clicks: "#fbbc04"
      conversions: "#34a853"
    stacking: ""
    row: 11
    col: 0
    width: 12
    height: 8

  - name: channel_roi_table
    title: "Channel ROI Comparison"
    type: table
    explore: channel_roi
    dimensions: [channel_roi.channel]
    measures:
      - channel_roi.total_spend
      - channel_roi.total_revenue
      - channel_roi.avg_roi
      - channel_roi.avg_roas
      - channel_roi.total_conversions
      - channel_roi.channel_health_score
      - channel_roi.best_campaign_name
    sorts: [channel_roi.avg_roi desc]
    conditional_formatting:
    - type: along a scale
      background_color: "#c8e6c9"
      bold: true
      value: 0
      field: channel_roi.avg_roi
    - type: greater than
      background_color: "#c8e6c9"
      bold: true
      value: 3
      field: channel_roi.avg_roas
    - type: less than
      background_color: "#ffcdd2"
      bold: true
      value: 1
      field: channel_roi.avg_roas
    row: 11
    col: 12
    width: 12
    height: 8

  # ---- Row 4: CAC by Channel + Scatter ----

  - name: cac_by_channel
    title: "Customer Acquisition Cost by Channel (EUR)"
    type: looker_bar
    explore: customer_acquisition
    dimensions: [customer_acquisition.channel]
    measures: [customer_acquisition.avg_cac]
    sorts: [customer_acquisition.avg_cac desc]
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    colors: ["#ea4335"]
    label_density: 25
    row: 19
    col: 0
    width: 12
    height: 8

  - name: revenue_vs_spend_scatter
    title: "Revenue vs Spend by Channel"
    type: looker_scatter
    explore: channel_roi
    dimensions: [channel_roi.channel]
    measures: [channel_roi.total_spend, channel_roi.total_revenue]
    x_axis_gridlines: true
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    x_axis_label: "Spend (EUR)"
    y_axis_label: "Revenue (EUR)"
    colors: ["#4285f4"]
    point_style: circle
    reference_lines:
    - reference_type: line
      line_value: mean
      label: "Avg Spend"
      color: "#fbbc04"
    row: 19
    col: 12
    width: 12
    height: 8

  # ---- Row 5: Customer Acquisition Pie + LTV/CAC Ratio ----

  - name: new_customers_by_channel
    title: "New Customers by Channel"
    type: looker_pie
    explore: customer_acquisition
    dimensions: [customer_acquisition.channel]
    measures: [customer_acquisition.total_new_customers]
    sorts: [customer_acquisition.total_new_customers desc]
    show_view_names: false
    colors: ["#4285f4", "#ea4335", "#fbbc04", "#34a852", "#673ab7", "#ff6d00"]
    inner_radius: 40
    row: 27
    col: 0
    width: 12
    height: 8

  - name: ltv_cac_ratio_by_channel
    title: "LTV/CAC Ratio by Channel"
    type: looker_bar
    explore: customer_acquisition
    dimensions: [customer_acquisition.channel]
    measures: [customer_acquisition.avg_ltv_cac_ratio]
    sorts: [customer_acquisition.avg_ltv_cac_ratio desc]
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    colors: ["#673ab7"]
    reference_lines:
    - reference_type: line
      line_value: "3.0"
      label: "Healthy (3x)"
      color: "#34a853"
    - reference_type: line
      line_value: "1.0"
      label: "Breakeven (1x)"
      color: "#ea4335"
    row: 27
    col: 12
    width: 12
    height: 8
