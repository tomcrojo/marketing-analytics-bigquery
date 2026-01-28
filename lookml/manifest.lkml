# manifest.lkml
# Marketing Analytics — Looker Project Configuration

project_name: "marketing-analytics"

connection: "marketing_analytics"

localization_settings: {
  default_locale: en
  localization_level: permissive
}

constant: project_id {
  value: "your-gcp-project-id"
  export: override_optional
}

constant: dataset {
  value: "analytics_marketing"
  export: override_optional
}

constant: schema_name {
  value: "analytics_marketing"
}
