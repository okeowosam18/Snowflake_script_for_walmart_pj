"""
Walmart Analytics - Visualization Script (No Browser Display)
Saves all visualizations to HTML files without opening browser
"""

import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# Create output directory
os.makedirs('output', exist_ok=True)

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

SNOWFLAKE_CONFIG = {
    'user': 'NIOMZOKEOWS',
    'password': 'MerryHousse99$',
    'account': 'JNUUWVE-WX20141',
    'warehouse': 'WALMART_WH',
    'database': 'WALMART_DB',
    'schema': 'bronze_analytics'  # e.g., 'PUBLIC' or 'ANALYTICS'
}

# ============================================================================
# CONNECTION AND QUERY FUNCTIONS
# ============================================================================

def get_connection():
    """Establish Snowflake connection"""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def execute_query(conn, query, description="Query"):
    """Execute query with error handling"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        print(f"✓ {description}: Retrieved {len(df):,} records")
        return df
    except Exception as e:
        print(f"✗ {description} failed: {e}")
        return None

# ============================================================================
# SQL QUERIES
# ============================================================================

QUERIES = {
    'sales_by_store_holiday': """
        SELECT 
            ft.Store_id,
            dd.Store_Date,
            dd.IsHoliday,
            SUM(ft.Store_Weekly_sales) as Total_Sales
        FROM walmart_fact_table ft
        JOIN walmart_date_dim dd ON ft.Date_id = dd.Date_id
        WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
        GROUP BY ft.Store_id, dd.Store_Date, dd.IsHoliday
        ORDER BY dd.Store_Date, ft.Store_id
    """,
    
    'sales_by_temp_year': """
        SELECT 
            YEAR(dd.Store_Date) as Year,
            ROUND(ft.Store_Temperature, 0) as Temperature,
            AVG(ft.Store_Weekly_sales) as Avg_Sales,
            COUNT(*) as Record_Count
        FROM walmart_fact_table ft
        JOIN walmart_date_dim dd ON ft.Date_id = dd.Date_id
        WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
            AND ft.Store_Temperature IS NOT NULL
        GROUP BY YEAR(dd.Store_Date), ROUND(ft.Store_Temperature, 0)
        ORDER BY Year, Temperature
    """,
    
    'sales_by_store_size': """
        SELECT 
            sd.Store_size,
            dd.Store_Date,
            AVG(ft.Store_Weekly_sales) as Avg_Sales,
            SUM(ft.Store_Weekly_sales) as Total_Sales,
            COUNT(DISTINCT ft.Store_id) as Store_Count
        FROM walmart_fact_table ft
        JOIN walmart_store_dim sd 
            ON ft.Store_id = sd.Store_id AND ft.Dept_id = sd.Dept_id
        JOIN walmart_date_dim dd ON ft.Date_id = dd.Date_id
        WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
            AND sd.Store_size IS NOT NULL
        GROUP BY sd.Store_size, dd.Store_Date
        ORDER BY dd.Store_Date, sd.Store_size
    """,
    
    'sales_by_type_month': """
        SELECT 
            sd.Store_type,
            DATE_TRUNC('month', dd.Store_Date) as Month,
            SUM(ft.Store_Weekly_sales) as Total_Sales,
            AVG(ft.Store_Weekly_sales) as Avg_Sales,
            COUNT(DISTINCT ft.Store_id) as Store_Count
        FROM walmart_fact_table ft
        JOIN walmart_store_dim sd 
            ON ft.Store_id = sd.Store_id AND ft.Dept_id = sd.Dept_id
        JOIN walmart_date_dim dd ON ft.Date_id = dd.Date_id
        WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
            AND sd.Store_type IS NOT NULL
        GROUP BY sd.Store_type, DATE_TRUNC('month', dd.Store_Date)
        ORDER BY Month, sd.Store_type
    """,
    
    'markdown_by_year_store': """
        SELECT 
            ft.Store_id,
            YEAR(dd.Store_Date) as Year,
            SUM(ft.MarkDown1) as Total_MarkDown1,
            SUM(ft.MarkDown2) as Total_MarkDown2,
            SUM(ft.MarkDown3) as Total_MarkDown3,
            SUM(ft.MarkDown4) as Total_MarkDown4,
            SUM(ft.MarkDown5) as Total_MarkDown5,
            SUM(ft.MarkDown1 + ft.MarkDown2 + ft.MarkDown3 + ft.MarkDown4 + ft.MarkDown5) as Total_Markdowns,
            SUM(ft.Store_Weekly_sales) as Total_Sales
        FROM walmart_fact_table ft
        JOIN walmart_date_dim dd ON ft.Date_id = dd.Date_id
        WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
        GROUP BY ft.Store_id, YEAR(dd.Store_Date)
        ORDER BY Year, ft.Store_id
    """
}

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def create_viz_1(df):
    """Visualization 1: Weekly Sales by Store and Holiday"""
    if df is None or df.empty:
        return None
    
    df_agg = df.groupby(['STORE_DATE', 'ISHOLIDAY'])['TOTAL_SALES'].sum().reset_index()
    fig = px.line(df_agg, x='STORE_DATE', y='TOTAL_SALES', color='ISHOLIDAY',
                  title='Weekly Sales by Holiday Status',
                  labels={'STORE_DATE': 'Date', 'TOTAL_SALES': 'Total Sales ($)', 
                         'ISHOLIDAY': 'Holiday Week'})
    fig.update_layout(template='plotly_white', hovermode='x unified')
    return fig

def create_viz_2(df):
    """Visualization 2: Weekly Sales by Temperature and Year"""
    if df is None or df.empty:
        return None
    
    fig = px.scatter(df, x='TEMPERATURE', y='AVG_SALES', color='YEAR',
                     size='RECORD_COUNT', title='Average Sales by Temperature Across Years',
                     labels={'TEMPERATURE': 'Temperature (°F)', 
                            'AVG_SALES': 'Average Weekly Sales ($)', 'YEAR': 'Year'},
                     trendline='ols')
    fig.update_layout(template='plotly_white')
    return fig

def create_viz_3(df):
    """Visualization 3: Weekly Sales by Store Size"""
    if df is None or df.empty:
        return None
    
    df['SIZE_BUCKET'] = pd.cut(df['STORE_SIZE'], 
                                bins=[0, 50000, 100000, 150000, 250000],
                                labels=['Small (<50K)', 'Medium (50-100K)', 
                                       'Large (100-150K)', 'Extra Large (>150K)'])
    fig = px.box(df, x='SIZE_BUCKET', y='AVG_SALES',
                 title='Sales Distribution by Store Size',
                 labels={'SIZE_BUCKET': 'Store Size Category', 
                        'AVG_SALES': 'Average Weekly Sales ($)'})
    fig.update_layout(template='plotly_white')
    return fig

def create_viz_4(df):
    """Visualization 4: Weekly Sales by Store Type and Month"""
    if df is None or df.empty:
        return None
    
    fig = px.line(df, x='MONTH', y='TOTAL_SALES', color='STORE_TYPE',
                  title='Monthly Sales Trend by Store Type',
                  labels={'MONTH': 'Month', 'TOTAL_SALES': 'Total Sales ($)',
                         'STORE_TYPE': 'Store Type'})
    fig.update_layout(template='plotly_white', hovermode='x unified')
    return fig

def create_viz_5a(df):
    """Visualization 5a: Markdown Distribution by Year"""
    if df is None or df.empty:
        return None
    
    df_agg = df.groupby('YEAR').agg({
        'TOTAL_MARKDOWN1': 'sum', 'TOTAL_MARKDOWN2': 'sum',
        'TOTAL_MARKDOWN3': 'sum', 'TOTAL_MARKDOWN4': 'sum',
        'TOTAL_MARKDOWN5': 'sum', 'TOTAL_SALES': 'sum'
    }).reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(name='MarkDown 1', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN1']))
    fig.add_trace(go.Bar(name='MarkDown 2', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN2']))
    fig.add_trace(go.Bar(name='MarkDown 3', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN3']))
    fig.add_trace(go.Bar(name='MarkDown 4', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN4']))
    fig.add_trace(go.Bar(name='MarkDown 5', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN5']))
    
    fig.update_layout(barmode='stack', title='Markdown Distribution by Year',
                     xaxis_title='Year', yaxis_title='Total Markdown Amount ($)',
                     template='plotly_white')
    return fig

def create_viz_5b(df):
    """Visualization 5b: Markdown vs Sales Correlation"""
    if df is None or df.empty:
        return None
    
    fig = px.scatter(df, x='TOTAL_MARKDOWNS', y='TOTAL_SALES', color='YEAR',
                     size='TOTAL_MARKDOWNS', hover_data=['STORE_ID'],
                     title='Total Markdowns vs Sales by Store and Year',
                     labels={'TOTAL_MARKDOWNS': 'Total Markdowns ($)', 
                            'TOTAL_SALES': 'Total Sales ($)', 'YEAR': 'Year'},
                     trendline='ols')
    fig.update_layout(template='plotly_white')
    return fig

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("=" * 80)
    print("WALMART ANALYTICS - VISUALIZATION GENERATION")
    print("=" * 80)
    
    # Connect to Snowflake
    print("\n[Step 1/7] Connecting to Snowflake...")
    try:
        conn = get_connection()
        print("✓ Connected successfully!")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return
    
    results = {}
    
    # Execute all queries
    print("\n[Step 2/7] Executing SQL queries...")
    for key, query in QUERIES.items():
        results[key] = execute_query(conn, query, f"Query: {key}")
    
    conn.close()
    print("✓ Database connection closed")
    
    # Create visualizations
    print("\n[Step 3/7] Creating Visualization 1: Sales by Holiday...")
    fig1 = create_viz_1(results['sales_by_store_holiday'])
    if fig1:
        fig1.write_html("output/viz_1_sales_by_store_holiday.html")
        print("✓ Saved: viz_1_sales_by_store_holiday.html")
    
    print("\n[Step 4/7] Creating Visualization 2: Sales by Temperature...")
    fig2 = create_viz_2(results['sales_by_temp_year'])
    if fig2:
        fig2.write_html("output/viz_2_sales_by_temp_year.html")
        print("✓ Saved: viz_2_sales_by_temp_year.html")
    
    print("\n[Step 5/7] Creating Visualization 3: Sales by Store Size...")
    fig3 = create_viz_3(results['sales_by_store_size'])
    if fig3:
        fig3.write_html("output/viz_3_sales_by_store_size.html")
        print("✓ Saved: viz_3_sales_by_store_size.html")
    
    print("\n[Step 6/7] Creating Visualization 4: Sales by Type and Month...")
    fig4 = create_viz_4(results['sales_by_type_month'])
    if fig4:
        fig4.write_html("output/viz_4_sales_by_type_month.html")
        print("✓ Saved: viz_4_sales_by_type_month.html")
    
    print("\n[Step 7/7] Creating Visualization 5: Markdown Analysis...")
    fig5a = create_viz_5a(results['markdown_by_year_store'])
    if fig5a:
        fig5a.write_html("output/viz_5a_markdown_by_year.html")
        print("✓ Saved: viz_5a_markdown_by_year.html")
    
    fig5b = create_viz_5b(results['markdown_by_year_store'])
    if fig5b:
        fig5b.write_html("output/viz_5b_markdown_vs_sales.html")
        print("✓ Saved: viz_5b_markdown_vs_sales.html")
    
    print("\n" + "=" * 80)
    print("✓ ALL VISUALIZATIONS COMPLETED!")
    print("=" * 80)
    print("\nHTML files saved in 'output/' directory:")
    print("  1. viz_1_sales_by_store_holiday.html")
    print("  2. viz_2_sales_by_temp_year.html")
    print("  3. viz_3_sales_by_store_size.html")
    print("  4. viz_4_sales_by_type_month.html")
    print("  5. viz_5a_markdown_by_year.html")
    print("  6. viz_5b_markdown_vs_sales.html")
    print("\nOpen these files in your browser to view the visualizations.")
    print()

if __name__ == "__main__":
    main()