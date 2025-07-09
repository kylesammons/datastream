import os
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import json
from datetime import datetime, date, timedelta
from streamlit_extras.metric_cards import style_metric_cards
import math

# Set Streamlit page config
st.set_page_config(
    page_title="DataStream", 
    page_icon="🚀", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# Field descriptions and exclusions for each dataset
FIELD_CONFIGURATIONS = {
    "Paid Media": {
        "descriptions": {
            "date": "Date of the data record",
            "Client_ID": "Internal TriMark ID assigned to client for reporting",
            "client_name": "Name of client",
            "Client_Group": "Roll up of Client Name (can have multiple clients)",
            "Business": "Roll up of Client Group (ex. Window World)",
            "Business_Model": "B2B or B2C",
            "Division": "Echo or Enterprise level",
            "Vertical": "Industry or vertical of the business (ex. Home Services)",
            "data_source_name": "Source or Ad Platform",
            "account_name": "Name of the advertising account",
            "account_id": "ID of the avertising account",
            "campaign_name": "Name of the advertising campaign",
            "campaign_id": "ID of the advertising campaign",
            "ad_type": "Ad Tactic (ex. Display or Search)",
            "impressions": "Number of times the ad was displayed",
            "video_views": "Number of times a video was watched or played",
            "video_impressions": "Number of times a video was displayed or shown to users",
            "clicks": "Number of clicks on the ad",
            "cost": "Total cost spent on advertising",
            "CPM": "Cost per thousand impressions",
            "CPC": "Cost per click",
            "CTR": "Click-through rate",
            "CPVV": "Cost per video view",
            "conversions": "Number of conversions tracked",
            "CPCONV": "Cost per conversion"
        },
        "excluded_fields": ["Comparison", "Waves", "is_year", "Frequency", "Primary_KPI"]
    },
    "GA4": {
        "descriptions": {
            "date": "Date of the data record",
            "Client_ID": "Internal TriMark ID assigned to client for reporting",
            "client_name": "Name of client",
            "Client_Group": "Roll up of Client Name (can have multiple clients)",
            "Business": "Roll up of Client Group (ex. Window World)",
            "Business_Model": "B2B or B2C",
            "Division": "Echo or Enterprise level",
            "Vertical": "Industry or vertical of the business (ex. Home Services)",
            "data_source_name": "GA4 Platform",
            "account_name": "Name of the GA4 account",
            "property_id": "ID of the GA4 property",
            "city": "City of the user",
            "eventName": "Name of the specific event tracked in GA4",
            "region": "Region of the user",
            "unifiedPagePath": "Standardized page path URL structure",
            "screenPageTitle": "Title of the page or screen viewed",
            "sessions": "Event Count where eventname = session_start",
            "sessionDefaultChannelGrouping": "Default channel grouping assigned to the session (e.g., Organic Search, Direct, Social)",
            "session_medium": "Medium through which the session was acquired (e.g., organic, cpc, referral)",
            "session_source": "Source that brought the user to the site (e.g., google, facebook, direct)",
            "user_engagement_duration": "Total time users spent actively engaged with the site",
            "views": "Total number of page or screen views",
            "conversions": "Number of goal completions",
            "landingPagePlusQueryString": "Landing page URL including query string parameters",
            "engaged_sessions": "Number of sessions that lasted 10+ seconds, had a conversion event, or had 2+ page views",
            "new_users": "Number of users who visited the site for the first time",
            "total_users": "Total number of unique users including both new and returning users"
        },
        "excluded_fields": ["Comparison", "Waves", "is_year", "Frequency", "Primary_KPI"]
    },
    "Leads": {
        "descriptions": {
            "date": "Date of the data record",
            "Client_ID": "Internal TriMark ID assigned to client for reporting",
            "Client_Name": "Name of client",
            "Client_Group": "Roll up of Client Name (can have multiple clients)",
            "Business": "Roll up of Client Group (ex. Window World)",
            "Business_Model": "B2B or B2C",
            "Division": "Echo or Enterprise level",
            "Vertical": "Industry or vertical of the business (ex. Home Services)",
            "Medium": "Marketing medium through which the lead was acquired (e.g., Paid Media or Organic)",
            "Source": "Traffic source that generated the lead (e.g., Google Ads, Facebook Ads etc.)",
            "form_leads": "From backup Gmail - Number of leads generated through form submissions",
            "call_leads": "From Marchex - Total number of leads generated through phone calls",
            "qualified_call_leads": "Marchex phone calls with qualification percentage applied",
            "organic_calls": "From GA4 - Organic or Referral traffic where eventName = 'click_to_call' OR  'Click-To-Call: Full Site'"
        },
        "excluded_fields": ["Comparison", "Waves", "is_year", "Frequency", "Primary_KPI"]
    }
}

# Dataset configurations
DATASETS = {
    "Paid Media": {
        "project_id": "trimark-tdp",
        "dataset_id": "master",
        "table_id": "all_paidmedia",
        "predefined_date_ranges": ["previous_month", "prior_month", "previous_month_previous_year"],
        "averaged_metrics": ['cpm', 'cpc', 'ctr', 'cpvv']
    },
    "GA4": {
        "project_id": "trimark-tdp",
        "dataset_id": "master",
        "table_id": "all_ga4",
        "predefined_date_ranges": ["previous_month", "prior_month", "previous_month_previous_year"],
        "averaged_metrics": []
    },
    "Leads": {
        "project_id": "trimark-tdp",
        "dataset_id": "master",
        "table_id": "all_leads",
        "predefined_date_ranges": ["previous_month", "prior_month", "previous_month_previous_year"],
        "averaged_metrics": []
    }
    # Add more datasets as needed
}

def format_field_name(field_name):
    """Format field names by removing underscores and capitalizing words"""
    return field_name.replace('_', ' ').title()

# BigQuery Creds - UPDATED VERSION
@st.cache_resource
def init_bigquery_client():
    """Initialize BigQuery client with service account credentials"""
    try:
        credentials = None
        
        # Try to load from Streamlit secrets
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            try:
                credentials = service_account.Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"]
                )
            except Exception as e:
                st.error(f"❌ Error loading credentials from Streamlit secrets: {str(e)}")
                return None
        else:
            st.error("❌ No GCP service account found in Streamlit secrets.")
            st.info("""
            🔧 **To fix this:**
            1. Go to your Streamlit app settings
            2. Navigate to 'Secrets' section  
            3. Add your service account JSON with key 'gcp_service_account'
            """)
            return None
        
        # Initialize client with credentials
        project_id = list(DATASETS.values())[0]["project_id"]
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Test the connection
        try:
            # Simple test query to verify connection works
            test_query = "SELECT 1 as test_connection"
            test_job = client.query(test_query)
            test_result = test_job.result()
            return client
        except Exception as e:
            st.error(f"❌ BigQuery connection test failed: {str(e)}")
            return None
            
    except Exception as e:
        st.error(f"❌ Error initializing BigQuery client: {str(e)}")
        return None

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_table_schema(dataset_config):
    """Get table schema for a specific dataset configuration"""
    client = init_bigquery_client()
    if not client:
        return {}, {}, []
    
    try:
        table_ref = client.dataset(dataset_config["dataset_id"]).table(dataset_config["table_id"])
        table = client.get_table(table_ref)
        
        dimensions = {}
        metrics = {}
        date_fields = []
        
        for field in table.schema:
            field_name = field.name
            field_type = field.field_type
            
            # Skip predefined date range boolean fields from dimensions
            if field_name in dataset_config["predefined_date_ranges"]:
                continue
                
            if field_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                date_fields.append(field_name)
                dimensions[field_name] = field_type
            elif field_type in ['STRING', 'BOOLEAN']:
                dimensions[field_name] = field_type
            elif field_type in ['INTEGER', 'FLOAT', 'NUMERIC']:
                metrics[field_name] = field_type
            else:
                dimensions[field_name] = field_type
        
        return dimensions, metrics, date_fields
    except Exception as e:
        st.error(f"Error getting table schema: {str(e)}")
        return {}, {}, []

@st.cache_data(ttl=3600)
def get_schema_for_display(dataset_name, dataset_config):
    """Get schema information formatted for display table"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    try:
        table_ref = client.dataset(dataset_config["dataset_id"]).table(dataset_config["table_id"])
        table = client.get_table(table_ref)
        
        schema_data = []
        field_config = FIELD_CONFIGURATIONS.get(dataset_name, {})
        descriptions = field_config.get("descriptions", {})
        excluded_fields = field_config.get("excluded_fields", [])
        
        for field in table.schema:
            field_name = field.name
            
            # Skip excluded fields
            if field_name in excluded_fields:
                continue
                
            # Skip predefined date range boolean fields
            if field_name in dataset_config["predefined_date_ranges"]:
                continue
            
            schema_data.append({
                "Field Name": format_field_name(field_name),
                "Data Type": field.field_type,
                "Description": descriptions.get(field_name, "No description available")
            })
        
        return pd.DataFrame(schema_data)
    except Exception as e:
        st.error(f"Error getting schema for display: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_schema_for_display(dataset_name, dataset_config):
    """Get schema information formatted for display table"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    try:
        table_ref = client.dataset(dataset_config["dataset_id"]).table(dataset_config["table_id"])
        table = client.get_table(table_ref)
        
        schema_data = []
        field_config = FIELD_CONFIGURATIONS.get(dataset_name, {})
        descriptions = field_config.get("descriptions", {})
        excluded_fields = field_config.get("excluded_fields", [])
        
        for field in table.schema:
            field_name = field.name
            field_type = field.field_type
            
            # Skip excluded fields
            if field_name in excluded_fields:
                continue
                
            # Skip predefined date range boolean fields
            if field_name in dataset_config["predefined_date_ranges"]:
                continue
            
            # Determine field category
            if field_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                field_category = "Date"
            elif field_type in ['STRING', 'BOOLEAN']:
                field_category = "Dimension"
            elif field_type in ['INTEGER', 'FLOAT', 'NUMERIC']:
                field_category = "Metric"
            else:
                field_category = "Dimension"  # Default fallback
            
            schema_data.append({
                "Field Name": format_field_name(field_name),
                "Field Type": field_category,
                "Data Type": field_type,
                "Description": descriptions.get(field_name, "No description available"),
                "original_name": field_name  # Keep original name for comparison
            })
        
        return pd.DataFrame(schema_data)
    except Exception as e:
        st.error(f"Error getting schema for display: {str(e)}")
        return pd.DataFrame()

def style_schema_dataframe(df, selected_dimensions, selected_metrics):
    """Apply styling to schema dataframe to highlight selected fields"""
    if df.empty:
        return df
    
    # Create a copy for styling
    styled_df = df.copy()
    
    # Remove the original_name column from display
    display_df = styled_df.drop('original_name', axis=1)
    
    # Create highlight function
    def highlight_selected_fields(row):
        original_name = styled_df.loc[row.name, 'original_name']
        
        # Check if field is selected
        if original_name in selected_dimensions or original_name in selected_metrics:
            return ['background-color: #e6f3ff; font-weight: bold'] * len(row)
        else:
            return [''] * len(row)
    
    # Apply styling
    styled = display_df.style.apply(highlight_selected_fields, axis=1)
    
    return styled

def build_query(dataset_config, start_date, end_date, predefined_range, dimensions, metrics, filters=None):
    """Build BigQuery SQL query based on dataset config and user selections"""
    
    # Build SELECT clause
    select_parts = []
    
    # Add dimensions
    if dimensions:
        select_parts.extend(dimensions)
    
    # Add metrics with aggregation (SUM or AVG based on dataset config)
    if metrics:
        for metric in metrics:
            metric_lower = metric.lower()
            if metric_lower in dataset_config["averaged_metrics"]:
                select_parts.append(f"AVG({metric}) as {metric}")
            else:
                select_parts.append(f"SUM({metric}) as {metric}")
    
    select_clause = ", ".join(select_parts)
    
    # Build FROM clause using dataset config
    from_clause = f"`{dataset_config['project_id']}.{dataset_config['dataset_id']}.{dataset_config['table_id']}`"
    
    # Build WHERE clause
    where_conditions = []
    
    # Add date filtering - either custom range or predefined range
    if predefined_range and predefined_range != "custom":
        # Use predefined boolean field
        where_conditions.append(f"{predefined_range} = TRUE")
    elif start_date and end_date:
        # Use custom date range (assuming date field is named 'date')
        where_conditions.append(f"date BETWEEN '{start_date}' AND '{end_date}'")
    
    # Add additional filters if provided
    if filters:
        where_conditions.extend(filters)
    
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    # Build GROUP BY clause
    group_by_clause = ""
    if dimensions and metrics:
        group_by_clause = "GROUP BY " + ", ".join(dimensions)
    
    # Build ORDER BY clause
    order_by_clause = ""
    if dimensions:
        order_by_clause = f"ORDER BY {dimensions[0]}"
    
    # Combine all parts
    query = f"""
    SELECT {select_clause}
    FROM {from_clause}
    {where_clause}
    {group_by_clause}
    {order_by_clause}
    """
    
    return query.strip()

def format_metric_value(metric_name, value):
    """Format metric values based on type - currency with $ and 2 decimals, others as whole numbers"""
    if pd.isna(value):
        return "0"
    
    # Check if it's a cost metric (contains cost, cpc, or cpm - case insensitive)
    metric_lower = metric_name.lower()
    is_currency = any(term in metric_lower for term in ['cost', 'cpc', 'cpm'])
    
    # Check if it's a rate metric (ends with _rate or contains rate)
    is_rate = 'rate' in metric_lower or metric_lower.endswith('_rate')
    
    if is_currency:
        return f"${value:,.2f}"
    elif is_rate:
        return f"{value:.2%}"
    else:
        return f"{int(value):,}"

def format_dataframe_for_display(df, selected_metrics):
    """Format dataframe values to match scorecard formatting"""
    display_df = df.copy()
    
    # Format column names
    display_df.columns = [format_field_name(col) for col in display_df.columns]
    
    # Format metric columns
    for col in display_df.columns:
        # Convert back to original name to check if it's a metric
        original_col = col.lower().replace(' ', '_')
        if any(original_col == metric.lower() for metric in selected_metrics):
            display_df[col] = display_df[col].apply(lambda x: format_metric_value(original_col, x))
    
    return display_df

def build_filter_condition(field, operator, value):
    """Build a WHERE condition based on field, operator, and value"""
    if operator == "equals":
        return f"{field} = '{value}'"
    elif operator == "contains":
        return f"{field} LIKE '%{value}%'"
    elif operator == "not_equals":
        return f"{field} != '{value}'"
    elif operator == "starts_with":
        return f"{field} LIKE '{value}%'"
    elif operator == "ends_with":
        return f"{field} LIKE '%{value}'"
    elif operator == "greater_than":
        return f"{field} > {value}"
    elif operator == "less_than":
        return f"{field} < {value}"
    elif operator == "greater_equal":
        return f"{field} >= {value}"
    elif operator == "less_equal":
        return f"{field} <= {value}"
    else:
        return f"{field} = '{value}'"

def execute_query(query, preview_limit=1000):
    """Execute BigQuery query and return results"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    try:
        # Add LIMIT for preview
        preview_query = f"{query} LIMIT {preview_limit}"
        
        job_config = bigquery.QueryJobConfig()
        query_job = client.query(preview_query, job_config=job_config)
        
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return pd.DataFrame()

def get_full_results(query):
    """Execute query without limit for download"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    try:
        job_config = bigquery.QueryJobConfig()
        query_job = client.query(query, job_config=job_config)
        
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error getting full results: {str(e)}")
        return pd.DataFrame()

# Initialize session state
if "query_results" not in st.session_state:
    st.session_state.query_results = pd.DataFrame()
if "current_query" not in st.session_state:
    st.session_state.current_query = ""
if "current_dataset" not in st.session_state:
    st.session_state.current_dataset = list(DATASETS.keys())[0]
if "filters" not in st.session_state:
    st.session_state.filters = []

# Sidebar for selections
with st.sidebar:
    st.image("Waves-Logo_Color.svg", width=200)
    st.markdown("<br>", unsafe_allow_html=True)
    st.header("🔧 Query Builder")
    
    # Dataset selection
    st.subheader("🗂️ Dataset")
    selected_dataset = st.selectbox(
        "Choose Dataset",
        options=list(DATASETS.keys()),
        help="Select which dataset to analyze"
    )
    
    # Clear results if dataset changed
    if selected_dataset != st.session_state.current_dataset:
        st.session_state.query_results = pd.DataFrame()
        st.session_state.current_query = ""
        st.session_state.filters = []
        st.session_state.current_dataset = selected_dataset
    
    # Get current dataset config
    current_dataset_config = DATASETS[selected_dataset]
    
    # Get table schema for selected dataset
    dimensions_dict, metrics_dict, date_fields = get_table_schema(current_dataset_config)
    
    # Filter out excluded fields
    field_config = FIELD_CONFIGURATIONS.get(selected_dataset, {})
    excluded_fields = field_config.get("excluded_fields", [])
    
    # Remove excluded fields from dimensions and metrics
    dimensions_dict = {k: v for k, v in dimensions_dict.items() if k not in excluded_fields}
    metrics_dict = {k: v for k, v in metrics_dict.items() if k not in excluded_fields}
    
    st.write("---")
    
    # Date range selection
    st.subheader("📅 Date Range")
    
    with st.expander("Select Date Range", expanded=False):
        # Date range type selection - dynamically based on dataset
        date_range_options = {"custom": "Custom Date Range"}
        for range_name in current_dataset_config["predefined_date_ranges"]:
            # Convert snake_case to Title Case for display
            display_name = range_name.replace('_', ' ').title()
            date_range_options[range_name] = display_name
        
        date_range_type = st.selectbox(
            "Date Range Type",
            options=list(date_range_options.keys()),
            format_func=lambda x: date_range_options[x],
            help="Choose between custom date range or predefined periods"
        )
        
        # Show custom date inputs only when custom is selected
        start_date = None
        end_date = None
        
        if date_range_type == "custom":
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=date.today() - timedelta(days=30),
                    help="Select start date"
                )
            with col2:
                end_date = st.date_input(
                    "End Date", 
                    value=date.today(),
                    help="Select end date"
                )
        else:
            # Show selected predefined range
            st.info(f"Using predefined range: {date_range_options[date_range_type]}")
    
    st.write("---")
    
    # Dimensions selection
    st.subheader("📋 Dimensions")
    
    with st.expander("Select Dimensions", expanded=False):
        st.caption("Select categorical fields to group by")
        
        selected_dimensions = st.multiselect(
            "Choose Dimensions",
            options=list(dimensions_dict.keys()),
            format_func=format_field_name,
            help="Select dimensions to group your data by"
        )
    
    st.write("---")
    
    # Metrics selection
    st.subheader("📈 Metrics")
    
    with st.expander("Select Metrics", expanded=False):
        st.caption("Select numeric fields to aggregate")
        
        selected_metrics = st.multiselect(
            "Choose Metrics",
            options=list(metrics_dict.keys()),
            format_func=format_field_name,
            help="Select metrics to sum up or average in your report"
        )
    
    st.write("---")
    
    # Filters section
    st.subheader("🔍 Filters")
    
    # Add new filter
    with st.expander("➕ Add Filter", expanded=False):
        st.caption("Add filters to narrow down your data")
        # Field selection on its own line
        all_fields = list(dimensions_dict.keys()) + list(metrics_dict.keys())
        filter_field = st.selectbox(
            "Field",
            options=all_fields,
            format_func=format_field_name,
            key="new_filter_field"
        )
        
        # Operator and Value on the same line
        col1, col2 = st.columns([1, 2])
        
        with col1:
            # Show different operators based on field type
            if filter_field in metrics_dict:
                # Numeric field operators
                operators = {
                    "equals": "=",
                    "greater_than": ">",
                    "less_than": "<",
                    "greater_equal": ">=",
                    "less_equal": "<=",
                    "not_equals": "!="
                }
            else:
                # String field operators
                operators = {
                    "equals": "=",
                    "contains": "contains",
                    "not_equals": "!=",
                    "starts_with": "starts with",
                    "ends_with": "ends with"
                }
            
            filter_operator = st.selectbox(
                "Operator",
                options=list(operators.keys()),
                format_func=lambda x: operators[x],
                key="new_filter_operator"
            )
        
        with col2:
            filter_value = st.text_input(
                "Value",
                key="new_filter_value"
            )
        
        if st.button("Add Filter", key="add_filter_btn"):
            if filter_field and filter_operator and filter_value:
                new_filter = {
                    "field": filter_field,
                    "operator": filter_operator,
                    "value": filter_value
                }
                st.session_state.filters.append(new_filter)
                st.success(f"Filter added: {format_field_name(filter_field)} {operators[filter_operator]} {filter_value}")
    
    # Display current filters
    if st.session_state.filters:
        st.write("**Current Filters:**")
        for i, filter_item in enumerate(st.session_state.filters):
            col1, col2 = st.columns([4, 1])
            with col1:
                operators_display = {
                    "equals": "=", "contains": "contains", "not_equals": "!=",
                    "starts_with": "starts with", "ends_with": "ends with",
                    "greater_than": ">", "less_than": "<", 
                    "greater_equal": ">=", "less_equal": "<="
                }
                st.write(f"• {format_field_name(filter_item['field'])} {operators_display[filter_item['operator']]} {filter_item['value']}")
            with col2:
                if st.button("🗑️", key=f"delete_filter_{i}", help="Delete filter"):
                    st.session_state.filters.pop(i)
                    st.rerun()
    
    st.write("---")
    
    # Query controls
    st.subheader("⚙️ Query Controls")
    
    with st.expander("Query Settings", expanded=False):
        preview_limit = st.slider(
            "Preview Limit",
            min_value=100,
            max_value=10000,
            value=1000,
            step=100,
            help="Limit rows for preview (full data available for download)"
        )
    
    st.write("---")
    
    # Build query button
    if st.button("🔍 Build Query", type="primary", use_container_width=True):
        if not selected_dimensions and not selected_metrics:
            st.error("Please select at least one dimension or metric")
        else:
            # Build filter conditions
            filter_conditions = []
            for filter_item in st.session_state.filters:
                condition = build_filter_condition(
                    filter_item['field'], 
                    filter_item['operator'], 
                    filter_item['value']
                )
                filter_conditions.append(condition)
            
            # Determine which date range to use
            predefined_range = date_range_type if date_range_type != "custom" else None
            
            query = build_query(
                dataset_config=current_dataset_config,
                start_date=start_date,
                end_date=end_date,
                predefined_range=predefined_range,
                dimensions=selected_dimensions,
                metrics=selected_metrics,
                filters=filter_conditions
            )
            st.session_state.current_query = query
            
            with st.spinner("Executing query..."):
                st.session_state.query_results = execute_query(query, preview_limit)
    
    # Clear results
    if st.button("🗑️ Clear Results", type="secondary", use_container_width=True):
        st.session_state.query_results = pd.DataFrame()
        st.session_state.current_query = ""
        st.session_state.filters = []

# Main content area
# App title
col1, col2 = st.columns([15, 1])
with col1:
    st.title("DataStream")
with col2:
    # Add spacing and make button less prominent
    st.write("")
    st.write("")  # Extra spacing to align with title
    with st.popover("ℹ️"):
        st.markdown(f"""
        #### How to use DataStream:
        1. Select a dataset - choose which data source to analyze
        2. Select a date range - choose either a custom date range or predefined period
        3. Choose dimensions - categorical fields to group your data by
        4. Choose metrics - numeric fields to aggregate (will be summed or averaged)
        5. Add filters - narrow down your data with field-specific filters
        6. Build your query and preview the results
        7. Download either the preview or full dataset
        """)

st.info(
    f"""Select your dataset, date range, dimensions, and metrics from the sidebar. Preview the data and download when ready!
    
    Current Dataset: {selected_dataset} ({current_dataset_config['dataset_id']}.{current_dataset_config['table_id']})
""",
    icon="🎯"
)


# Show data schema - hide in expander if query results exist
schema_df = get_schema_for_display(selected_dataset, current_dataset_config)
if not schema_df.empty:
    if not st.session_state.query_results.empty:
        # Hide schema in expander when results are available
        with st.expander("📊 Data Schema", expanded=False):
            # Apply styling to highlight selected fields
            styled_schema = style_schema_dataframe(schema_df, selected_dimensions, selected_metrics)
            st.dataframe(
                styled_schema,
                use_container_width=True,
                hide_index=True
            )
    else:  
        st.subheader(f"{selected_dataset} Data Schema")
        # Apply styling to highlight selected fields
        styled_schema = style_schema_dataframe(schema_df, selected_dimensions, selected_metrics)
        st.dataframe(
            styled_schema,
            use_container_width=True,
            hide_index=True
        )
else:
    st.warning("Unable to load schema information.")

# Show query results if available
if not st.session_state.query_results.empty:
    
    # Show query expander right after results
    with st.expander("📝 View Generated SQL Query", expanded=False):
        st.code(st.session_state.current_query, language="sql")
    
    # Data preview
    st.subheader("Data Preview")
    
    # Display metrics summary if available
    if selected_metrics:
        st.write("Metrics Summary:")
        
        # Apply metric card styling
        style_metric_cards(
            border_left_color="#6DC6DB", 
            border_color="#FFFFFF", 
            background_color=None
        )
        
        # Calculate metrics summary
        metrics_summary = {}
        for metric in selected_metrics:
            if metric in st.session_state.query_results.columns:
                metric_lower = metric.lower()
                if metric_lower in current_dataset_config["averaged_metrics"]:
                    # For averaged metrics, calculate the mean
                    total = st.session_state.query_results[metric].mean()
                    formatted_value = format_metric_value(metric, total)
                    metrics_summary[f"Avg {format_field_name(metric)}"] = formatted_value
                else:
                    # For summed metrics, calculate the sum
                    total = st.session_state.query_results[metric].sum()
                    formatted_value = format_metric_value(metric, total)
                    metrics_summary[f"Total {format_field_name(metric)}"] = formatted_value
        
        # Display metrics in rows of 3
        if metrics_summary:
            metrics_items = list(metrics_summary.items())
            
            # Calculate number of rows needed (3 columns per row)
            num_rows = math.ceil(len(metrics_items) / 3)
            
            for row in range(num_rows):
                # Get up to 3 items for this row
                start_idx = row * 3
                end_idx = min(start_idx + 3, len(metrics_items))
                row_items = metrics_items[start_idx:end_idx]
                
                # Create columns for this row
                cols = st.columns(3)
                
                # Display metrics in columns
                for col_idx, (key, value) in enumerate(row_items):
                    cols[col_idx].metric(key, value)
    
    # Add space between scorecards and table
    st.write("")
    st.write("")
    
    # Create formatted dataframe for display
    display_df = format_dataframe_for_display(st.session_state.query_results, selected_metrics)
    
    # Display data table
    st.dataframe(
        display_df,
        use_container_width=True,
        height=400
    )
    
    # Download section
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Single download button for full dataset - immediate download
        if not st.session_state.query_results.empty:
            # Get full results for download
            full_results = get_full_results(st.session_state.current_query)
            if not full_results.empty:
                csv_full = full_results.to_csv(index=False)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{selected_dataset.lower().replace(' ', '_')}_data_{timestamp}.csv"
                st.download_button(
                    label="📥 Download Dataset",
                    data=csv_full,
                    file_name=filename,
                    mime="text/csv",
                    type="primary"
                )
