# 🚀 Crypto Market Performance & Volatility Analysis (Dec 2025)

An end-to-end data analytics project using **Python**, **SQL (MySQL)**, and **Power BI** to evaluate risk-reward dynamics in high-frequency cryptocurrency data.

## 📊 Project Overview
This project analyzes 1-minute interval data for various crypto assets to identify market trends, liquidity zones, and volatility spikes. The goal is to provide a standardized "Growth Index" to compare assets regardless of their unit price.

## 🛠️ Technical Stack
- **Python (Pandas/NumPy):** Data cleaning, handling missing values, and timestamp normalization.
- **SQL (Window Functions):** Computed 7-day Moving Averages, Daily Returns, and Asset Rankings using `DENSE_RANK()` and `LAG()`.
- **Power BI:** Built an interactive dashboard featuring:
  - **Growth Index (Base 100):** For normalized performance comparison.
  - **Risk-Reward Scatter Matrix:** To categorize assets by volatility.
  - **Treemap:** For Market Dominance (Volume) analysis.

## 💡 Key Insights
- **Performance:** Identified assets with >15% growth from a normalized base.
- **Risk:** Altcoins exhibited 20% higher volatility than BTC, requiring tighter stop-loss strategies.
- **Liquidity:** 70% of market activity was concentrated in top-tier assets, despite price action in memecoins.

## 📂 Repository Structure
- `Crypto_Analytics.ipynb`: Python notebook for EDA & Cleaning.
- `Crypto Analytics SQL.sql`: SQL scripts for complex aggregations.
- `Crypto_Performance_Dashboard.pbix`: Power BI file.
- `Documentation.pdf`: Professional project report.
- `Crypto_Analytics PPT.pdf`: Professional project PPT.

---
📫 **Let's Connect!** [www.linkedin.com/in/arnavgurung]
