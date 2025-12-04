"""
Walmart Analytics - Complete Visualization Script
Python queries and visualizations for Walmart sales analysis
"""

import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================

def get_snowflake_connection():
    """
    Establish connection to Snowflake
    Update with your credentials
    """
    conn = snowflake.connector.connect(
        user='NIOMZOKEOWS',
        password='MerryHousse99$',
        account='JNUUWVE-WX20141',
        warehouse='WALMART_WH',
        database='WALMART_DB',
        schema='bronze_analytics'  # Update this - likely 'PUBLIC' or check your dbt schema
    )
    return conn

def execute_query(conn, query):
    """
    Execute SQL query and return DataFrame
    """
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    return df

# ============================================================================
# SQL QUERIES
# ============================================================================

# 1. Weekly Sales by Store and Holiday
query_sales_by_store_holiday = """
SELECT 
    ft.Store_id,
    dd.Store_Date,
    dd.IsHoliday,
    SUM(ft.Store_Weekly_sales) as Total_Sales
FROM analytics.walmart_fact_table ft
JOIN analytics.walmart_date_dim dd 
    ON ft.Date_id = dd.Date_id
WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
GROUP BY ft.Store_id, dd.Store_Date, dd.IsHoliday
ORDER BY dd.Store_Date, ft.Store_id
"""

# 2. Weekly Sales by Temperature and Year
query_sales_by_temp_year = """
SELECT 
    YEAR(dd.Store_Date) as Year,
    ROUND(ft.Store_Temperature, 0) as Temperature,
    AVG(ft.Store_Weekly_sales) as Avg_Sales,
    COUNT(*) as Record_Count
FROM analytics.walmart_fact_table ft
JOIN analytics.walmart_date_dim dd 
    ON ft.Date_id = dd.Date_id
WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
    AND ft.Store_Temperature IS NOT NULL
GROUP BY YEAR(dd.Store_Date), ROUND(ft.Store_Temperature, 0)
ORDER BY Year, Temperature
"""

# 3. Weekly Sales by Store Size
query_sales_by_store_size = """
SELECT 
    sd.Store_size,
    dd.Store_Date,
    AVG(ft.Store_Weekly_sales) as Avg_Sales,
    SUM(ft.Store_Weekly_sales) as Total_Sales,
    COUNT(DISTINCT ft.Store_id) as Store_Count
FROM analytics.walmart_fact_table ft
JOIN analytics.walmart_store_dim sd 
    ON ft.Store_id = sd.Store_id AND ft.Dept_id = sd.Dept_id
JOIN analytics.walmart_date_dim dd 
    ON ft.Date_id = dd.Date_id
WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
    AND sd.Store_size IS NOT NULL
GROUP BY sd.Store_size, dd.Store_Date
ORDER BY dd.Store_Date, sd.Store_size
"""

# 4. Weekly Sales by Store Type and Month
query_sales_by_type_month = """
SELECT 
    sd.Store_type,
    DATE_TRUNC('month', dd.Store_Date) as Month,
    SUM(ft.Store_Weekly_sales) as Total_Sales,
    AVG(ft.Store_Weekly_sales) as Avg_Sales,
    COUNT(DISTINCT ft.Store_id) as Store_Count
FROM analytics.walmart_fact_table ft
JOIN analytics.walmart_store_dim sd 
    ON ft.Store_id = sd.Store_id AND ft.Dept_id = sd.Dept_id
JOIN analytics.walmart_date_dim dd 
    ON ft.Date_id = dd.Date_id
WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
    AND sd.Store_type IS NOT NULL
GROUP BY sd.Store_type, DATE_TRUNC('month', dd.Store_Date)
ORDER BY Month, sd.Store_type
"""

# 5. Markdown Sales by Year and Store
query_markdown_by_year_store = """
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
FROM analytics.walmart_fact_table ft
JOIN analytics.walmart_date_dim dd 
    ON ft.Date_id = dd.Date_id
WHERE ft.Vrsn_end_date = '9999-12-31 23:59:59'
GROUP BY ft.Store_id, YEAR(dd.Store_Date)
ORDER BY Year, ft.Store_id
"""

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def viz_1_sales_by_store_holiday(df):
    """
    Visualization 1: Weekly Sales by Store and Holiday
    Line chart showing sales trends with holiday indicators
    """
    # Aggregate by date and holiday
    df_agg = df.groupby(['STORE_DATE', 'ISHOLIDAY'])['TOTAL_SALES'].sum().reset_index()
    
    fig = px.line(df_agg, 
                  x='STORE_DATE', 
                  y='TOTAL_SALES',
                  color='ISHOLIDAY',
                  title='Weekly Sales by Holiday Status',
                  labels={'STORE_DATE': 'Date', 
                         'TOTAL_SALES': 'Total Sales ($)',
                         'ISHOLIDAY': 'Holiday Week'})
    
    fig.update_layout(
        template='plotly_white',
        hovermode='x unified',
        xaxis_title='Date',
        yaxis_title='Total Sales ($)'
    )
    
    return fig

def viz_2_sales_by_temp_year(df):
    """
    Visualization 2: Weekly Sales by Temperature and Year
    Scatter plot with color-coded years
    """
    fig = px.scatter(df, 
                     x='TEMPERATURE', 
                     y='AVG_SALES',
                     color='YEAR',
                     size='RECORD_COUNT',
                     title='Average Sales by Temperature Across Years',
                     labels={'TEMPERATURE': 'Temperature (°F)', 
                            'AVG_SALES': 'Average Weekly Sales ($)',
                            'YEAR': 'Year'},
                     trendline='ols')
    
    fig.update_layout(
        template='plotly_white',
        xaxis_title='Temperature (°F)',
        yaxis_title='Average Weekly Sales ($)'
    )
    
    return fig

def viz_3_sales_by_store_size(df):
    """
    Visualization 3: Weekly Sales by Store Size
    Box plot showing sales distribution by store size
    """
    # Create size buckets for better visualization
    df['SIZE_BUCKET'] = pd.cut(df['STORE_SIZE'], 
                                bins=[0, 50000, 100000, 150000, 250000],
                                labels=['Small (<50K)', 'Medium (50-100K)', 
                                       'Large (100-150K)', 'Extra Large (>150K)'])
    
    fig = px.box(df, 
                 x='SIZE_BUCKET', 
                 y='AVG_SALES',
                 title='Sales Distribution by Store Size',
                 labels={'SIZE_BUCKET': 'Store Size Category', 
                        'AVG_SALES': 'Average Weekly Sales ($)'})
    
    fig.update_layout(
        template='plotly_white',
        xaxis_title='Store Size Category',
        yaxis_title='Average Weekly Sales ($)'
    )
    
    return fig

def viz_4_sales_by_type_month(df):
    """
    Visualization 4: Weekly Sales by Store Type and Month
    Line chart showing monthly trends by store type
    """
    fig = px.line(df, 
                  x='MONTH', 
                  y='TOTAL_SALES',
                  color='STORE_TYPE',
                  title='Monthly Sales Trend by Store Type',
                  labels={'MONTH': 'Month', 
                         'TOTAL_SALES': 'Total Sales ($)',
                         'STORE_TYPE': 'Store Type'})
    
    fig.update_layout(
        template='plotly_white',
        hovermode='x unified',
        xaxis_title='Month',
        yaxis_title='Total Sales ($)'
    )
    
    return fig

def viz_5_markdown_by_year_store(df):
    """
    Visualization 5: Markdown Sales by Year and Store
    Stacked bar chart showing markdown distribution
    """
    # Aggregate by year for clearer visualization
    df_agg = df.groupby('YEAR').agg({
        'TOTAL_MARKDOWN1': 'sum',
        'TOTAL_MARKDOWN2': 'sum',
        'TOTAL_MARKDOWN3': 'sum',
        'TOTAL_MARKDOWN4': 'sum',
        'TOTAL_MARKDOWN5': 'sum',
        'TOTAL_SALES': 'sum'
    }).reset_index()
    
    fig = go.Figure()
    
    # Add traces for each markdown
    fig.add_trace(go.Bar(name='MarkDown 1', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN1']))
    fig.add_trace(go.Bar(name='MarkDown 2', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN2']))
    fig.add_trace(go.Bar(name='MarkDown 3', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN3']))
    fig.add_trace(go.Bar(name='MarkDown 4', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN4']))
    fig.add_trace(go.Bar(name='MarkDown 5', x=df_agg['YEAR'], y=df_agg['TOTAL_MARKDOWN5']))
    
    fig.update_layout(
        barmode='stack',
        title='Markdown Distribution by Year',
        xaxis_title='Year',
        yaxis_title='Total Markdown Amount ($)',
        template='plotly_white',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

def viz_5_alternate_markdown_by_store(df):
    """
    Visualization 5 (Alternate): Markdown vs Sales by Store
    Scatter plot showing relationship between markdowns and sales
    """
    fig = px.scatter(df, 
                     x='TOTAL_MARKDOWNS', 
                     y='TOTAL_SALES',
                     color='YEAR',
                     size='TOTAL_MARKDOWNS',
                     hover_data=['STORE_ID'],
                     title='Total Markdowns vs Sales by Store and Year',
                     labels={'TOTAL_MARKDOWNS': 'Total Markdowns ($)', 
                            'TOTAL_SALES': 'Total Sales ($)',
                            'YEAR': 'Year'},
                     trendline='ols')
    
    fig.update_layout(
        template='plotly_white',
        xaxis_title='Total Markdowns ($)',
        yaxis_title='Total Sales ($)'
    )
    
    return fig

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function
    """
    print("=" * 80)
    print("WALMART ANALYTICS - VISUALIZATION SCRIPT")
    print("=" * 80)
    
    # Connect to Snowflake
    print("\n[1/6] Connecting to Snowflake...")
    conn = get_snowflake_connection()
    print("✓ Connected successfully!")
    
    # Execute queries and create visualizations
    figures = []
    
    try:
        # Visualization 1: Sales by Store and Holiday
        print("\n[2/6] Creating Visualization 1: Weekly Sales by Store and Holiday...")
        df1 = execute_query(conn, query_sales_by_store_holiday)
        fig1 = viz_1_sales_by_store_holiday(df1)
        fig1.write_html("output/viz_1_sales_by_store_holiday.html")
        figures.append(('Viz 1: Sales by Holiday', fig1))
        print(f"✓ Processed {len(df1):,} records")
    except Exception as e:
        print(f"✗ Error in Visualization 1: {e}")
    
    try:
        # Visualization 2: Sales by Temperature and Year
        print("\n[3/6] Creating Visualization 2: Weekly Sales by Temperature and Year...")
        df2 = execute_query(conn, query_sales_by_temp_year)
        fig2 = viz_2_sales_by_temp_year(df2)
        fig2.write_html("output/viz_2_sales_by_temp_year.html")
        figures.append(('Viz 2: Sales by Temperature', fig2))
        print(f"✓ Processed {len(df2):,} records")
    except Exception as e:
        print(f"✗ Error in Visualization 2: {e}")
    
    try:
        # Visualization 3: Sales by Store Size
        print("\n[4/6] Creating Visualization 3: Weekly Sales by Store Size...")
        df3 = execute_query(conn, query_sales_by_store_size)
        fig3 = viz_3_sales_by_store_size(df3)
        fig3.write_html("output/viz_3_sales_by_store_size.html")
        figures.append(('Viz 3: Sales by Store Size', fig3))
        print(f"✓ Processed {len(df3):,} records")
    except Exception as e:
        print(f"✗ Error in Visualization 3: {e}")
    
    try:
        # Visualization 4: Sales by Store Type and Month
        print("\n[5/6] Creating Visualization 4: Weekly Sales by Store Type and Month...")
        df4 = execute_query(conn, query_sales_by_type_month)
        fig4 = viz_4_sales_by_type_month(df4)
        fig4.write_html("output/viz_4_sales_by_type_month.html")
        figures.append(('Viz 4: Sales by Type & Month', fig4))
        print(f"✓ Processed {len(df4):,} records")
    except Exception as e:
        print(f"✗ Error in Visualization 4: {e}")
    
    try:
        # Visualization 5: Markdown by Year and Store
        print("\n[6/6] Creating Visualization 5: Markdown Sales by Year and Store...")
        df5 = execute_query(conn, query_markdown_by_year_store)
        fig5a = viz_5_markdown_by_year_store(df5)
        fig5a.write_html("output/viz_5a_markdown_by_year.html")
        figures.append(('Viz 5a: Markdown by Year', fig5a))
        
        fig5b = viz_5_alternate_markdown_by_store(df5)
        fig5b.write_html("output/viz_5b_markdown_vs_sales.html")
        figures.append(('Viz 5b: Markdown vs Sales', fig5b))
        print(f"✓ Processed {len(df5):,} records")
    except Exception as e:
        print(f"✗ Error in Visualization 5: {e}")
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 80)
    print("✓ ALL VISUALIZATIONS COMPLETED!")
    print("=" * 80)
    print("\nOutput files saved in 'output/' directory:")
    print("  - viz_1_sales_by_store_holiday.html")
    print("  - viz_2_sales_by_temp_year.html")
    print("  - viz_3_sales_by_store_size.html")
    print("  - viz_4_sales_by_type_month.html")
    print("  - viz_5a_markdown_by_year.html")
    print("  - viz_5b_markdown_vs_sales.html")
    
    # Display all figures at once (optional - comment out if not needed)
    print("\n[OPTIONAL] Opening visualizations in browser...")
    print("Close each browser window to see the next visualization.")
    for title, fig in figures:
        print(f"\nShowing: {title}")
        fig.show()
    
    print("\n✓ All done!\n")

# ============================================================================
# RUN INDIVIDUAL VISUALIZATIONS
# ============================================================================

def run_single_viz(viz_number):
    """
    Run a single visualization
    Usage: run_single_viz(1) for visualization 1
    """
    conn = get_snowflake_connection()
    
    if viz_number == 1:
        df = execute_query(conn, query_sales_by_store_holiday)
        fig = viz_1_sales_by_store_holiday(df)
    elif viz_number == 2:
        df = execute_query(conn, query_sales_by_temp_year)
        fig = viz_2_sales_by_temp_year(df)
    elif viz_number == 3:
        df = execute_query(conn, query_sales_by_store_size)
        fig = viz_3_sales_by_store_size(df)
    elif viz_number == 4:
        df = execute_query(conn, query_sales_by_type_month)
        fig = viz_4_sales_by_type_month(df)
    elif viz_number == 5:
        df = execute_query(conn, query_markdown_by_year_store)
        fig = viz_5_markdown_by_year_store(df)
    else:
        print(f"Invalid visualization number: {viz_number}")
        return
    
    conn.close()
    fig.show()
    print(f"✓ Visualization {viz_number} displayed")

if __name__ == "__main__":
    # Create output directory if it doesn't exist
    import os
    os.makedirs('output', exist_ok=True)
    
    # Run all visualizations
    main()
    
    # Or run individual visualization:
    # run_single_viz(1)  # Uncomment to run only viz 1