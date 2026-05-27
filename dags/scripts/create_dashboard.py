import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def create_dashboard(duckdb_path: str, output_path: str):
    con = duckdb.connect(duckdb_path)

    top_customers = con.execute("""
        SELECT customer_id, ROUND(amount_spent, 2) as amount_spent
        FROM user_movie_review
        WHERE amount_spent > 0
        ORDER BY amount_spent DESC
        LIMIT 10
    """).df()

    review_dist = con.execute("""
        SELECT 
            CASE 
                WHEN num_reviews = 0 THEN 'Không review'
                WHEN num_reviews <= 10 THEN '1-10 reviews'
                WHEN num_reviews <= 50 THEN '11-50 reviews'
                WHEN num_reviews <= 100 THEN '51-100 reviews'
                ELSE 'Trên 100 reviews'
            END as review_group,
            COUNT(*) as num_customers
        FROM user_movie_review
        GROUP BY review_group
        ORDER BY num_customers DESC
    """).df()

    summary = con.execute("""
        SELECT 
            COUNT(DISTINCT customer_id) as total_customers,
            ROUND(SUM(amount_spent), 2) as total_revenue,
            ROUND(AVG(amount_spent), 2) as avg_spent
        FROM user_movie_review
    """).fetchone()
    con.close()

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Top 10 khách hàng chi nhiều nhất",
            "Phân phối số reviews",
            "Tổng khách hàng",
            "Doanh thu trung bình / khách"
        ),
        specs=[
            [{"type": "bar"}, {"type": "pie"}],
            [{"type": "indicator"}, {"type": "indicator"}]
        ],
        vertical_spacing=0.15,
        horizontal_spacing=0.1
    )

    # Bar chart
    fig.add_trace(go.Bar(
        x=[str(c) for c in top_customers["customer_id"]],
        y=top_customers["amount_spent"],
        marker=dict(
            color=top_customers["amount_spent"],
            colorscale="Blues",
            showscale=False
        ),
        name="Doanh thu"
    ), row=1, col=1)

    # Pie chart
    colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]
    fig.add_trace(go.Pie(
        labels=review_dist["review_group"],
        values=review_dist["num_customers"],
        marker=dict(colors=colors),
        hole=0.4,
        name="Reviews"
    ), row=1, col=2)

    # Indicator 1
    fig.add_trace(go.Indicator(
        mode="number",
        value=summary[0],
        number={"font": {"size": 72, "color": "#636EFA"}, "suffix": " KH"},
        title={"text": "Tổng khách hàng", "font": {"size": 16}}
    ), row=2, col=1)

    # Indicator 2
    fig.add_trace(go.Indicator(
        mode="number",
        value=summary[2],
        number={"font": {"size": 72, "color": "#00CC96"}, "prefix": "$",
                "valueformat": ",.0f"},
        title={"text": "Doanh thu TB / khách", "font": {"size": 16}}
    ), row=2, col=2)

    fig.update_layout(
        title=dict(
            text="<b>User Analytics Dashboard</b>",
            font=dict(size=28, color="#2c3e50"),
            x=0.5
        ),
        height=750,
        paper_bgcolor="#f8f9fa",
        plot_bgcolor="#ffffff",
        font=dict(family="Arial, sans-serif", size=13),
        showlegend=True,
        legend=dict(
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#dee2e6",
            borderwidth=1
        )
    )

    fig.update_xaxes(tickangle=-45, row=1, col=1)
    fig.update_yaxes(tickformat="$,.0f", row=1, col=1)

    fig.write_html(output_path)
    print(f"Dashboard đã tạo tại: {output_path}")

if __name__ == "__main__":
    import sys
    create_dashboard(sys.argv[1], sys.argv[2])
