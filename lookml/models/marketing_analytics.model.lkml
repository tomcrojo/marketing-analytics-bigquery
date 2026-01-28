# marketing_analytics.model.lkml
# Marketing Analytics Model — BigQuery connection, explores, and access

connection: "marketing_analytics"

include: "/views/*.view.lkml"
include: "/dashboards/*.dashboard.lkml"

# ---------------------------------------------------------------------------
# Marketing Dashboard Explore
# ---------------------------------------------------------------------------
explore: marketing_dashboard {
  label: "Marketing Performance Dashboard"
  description: "Daily marketing performance metrics across all channels and platforms"
  group_label: "Marketing Analytics"

  always_filter: {
    filters: [marketing_dashboard.date: "last 90 days"]
  }

  access_filter: {
    field: marketing_dashboard.channel
    user_attribute: allowed_channels
  }
}

# ---------------------------------------------------------------------------
# Channel ROI Explore
# ---------------------------------------------------------------------------
explore: channel_roi {
  label: "Channel ROI Analysis"
  description: "Channel-level ROI comparison with best/worst campaign identification"
  group_label: "Marketing Analytics"
}

# ---------------------------------------------------------------------------
# Customer Acquisition Explore
# ---------------------------------------------------------------------------
explore: customer_acquisition {
  label: "Customer Acquisition Funnel"
  description: "CAC, LTV, payback period, and acquisition health by channel"
  group_label: "Marketing Analytics"

  always_filter: {
    filters: [customer_acquisition.acquisition_month: "last 12 months"]
  }
}
