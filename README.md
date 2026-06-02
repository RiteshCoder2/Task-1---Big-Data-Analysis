# 🛒 Indian E-Commerce Sales Big Data Analysis using Dask

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Dask](https://img.shields.io/badge/Dask-Big%20Data-EF7C35?style=for-the-badge)
![Pandas](https://img.shields.io/badge/Pandas-Analysis-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?style=for-the-badge&logo=jupyter&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-Visualization-11557C?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

</div>

---

## 📋 Internship Details

| Field | Details |
|-------|---------|
| **Name** | Ritesh Chandra |
| **Company** | CODTECH IT Solutions Pvt. Ltd. |
| **Intern ID** | CITS419 |
| **Domain** | Data Analytics |
| **Duration** | 4 Weeks |
| **Mentor** | Neela Santhosh Kumar |
| **Task** | Task 1 — Big Data Analysis |
---


## 🎯 Project Overview

> Perform **Big Data Analysis** on a large Indian E-Commerce dataset (50,000 orders × 48 columns) using **Dask** for parallel and scalable data processing. Derive actionable business insights across sales, revenue, customer behavior, regional performance, and platform analytics.

This project demonstrates that the same Dask-based code scales from 50,000 rows to **50 million+ rows** on a distributed cluster without any code changes — proving true Big Data scalability.

---

## 🗂️ Project Structure

```
Task-1-Big-Data-Analysis/
│
├── 📓 big_data_analysis.ipynb     ← Main Jupyter Notebook (43 cells, 13 sections)
├── 📊 ecommerce_big_data.csv      ← Dataset (50,000 rows × 48 columns)
└── 📝 README.md
```

---

## 📊 Dataset Description

**File:** `ecommerce_big_data.csv`
**Records:** 50,000 rows | **Columns:** 48

### Column Reference

| # | Column | Type | Description |
|---|--------|------|-------------|
| 1 | `order_id` | str | Unique order identifier (ORD000001…) |
| 2 | `order_date` | date | Order placement date (2022–2024) |
| 3 | `order_month` | str | Month name (Jan–Dec) |
| 4 | `order_quarter` | str | Quarter (Q1–Q4) |
| 5 | `order_year` | int | Year (2022, 2023, 2024) |
| 6 | `customer_id` | str | Unique customer ID (CUST#####) |
| 7 | `customer_name_initial` | str | Customer initials (privacy preserved) |
| 8 | `customer_age_group` | str | Age bracket (18-24 to 65+) |
| 9 | `customer_gender` | str | M / F |
| 10 | `customer_occupation` | str | Student / Salaried / Self-Employed / etc. |
| 11 | `customer_segment` | str | New / Regular / Premium / VIP / Inactive |
| 12 | `customer_since_year` | int | Year of first purchase |
| 13 | `city` | str | Delivery city |
| 14 | `state` | str | Delivery state (15 Indian states) |
| 15 | `pincode` | int | Delivery pincode |
| 16 | `tier` | str | City tier (Tier 1 / Tier 2 / Tier 3) |
| 17 | `platform` | str | E-commerce platform (Amazon, Flipkart, etc.) |
| 18 | `device_type` | str | Mobile / Desktop / Tablet / App-Android / App-iOS |
| 19 | `traffic_source` | str | Organic / Paid Ad / Social Media / Email / etc. |
| 20 | `category` | str | Product category (10 categories) |
| 21 | `subcategory` | str | Product subcategory (80 subcategories) |
| 22 | `brand` | str | Product brand |
| 23 | `product_id` | str | Unique product ID (PROD######) |
| 24 | `product_name_code` | str | Short product code |
| 25 | `unit_price_inr` | float | MRP per unit (₹) |
| 26 | `quantity` | int | Units ordered (1–5) |
| 27 | `discount_percent` | int | Discount applied (0–50%) |
| 28 | `discount_amount_inr` | float | Discount value in ₹ |
| 29 | `selling_price_inr` | float | Price after discount |
| 30 | `gst_percent` | int | GST rate (5/12/18/28%) |
| 31 | `gst_amount_inr` | float | GST charged in ₹ |
| 32 | `total_amount_inr` | float | Final order total (₹) |
| 33 | `payment_method` | str | UPI / Credit Card / EMI / COD / etc. |
| 34 | `emi_months` | int | EMI tenure (0, 3, 6, 9, 12 months) |
| 35 | `wallet_name` | str | Paytm / PhonePe / Google Pay / etc. |
| 36 | `shipping_method` | str | Standard / Express / Same Day / etc. |
| 37 | `shipping_cost_inr` | float | Shipping fee charged (₹) |
| 38 | `delivery_days` | int | Actual delivery time (days) |
| 39 | `order_status` | str | Delivered / Shipped / Cancelled / Returned / etc. |
| 40 | `return_flag` | int | 1 = Returned/Refunded, 0 = Not returned |
| 41 | `return_reason` | str | Defective / Wrong Item / Changed Mind / etc. |
| 42 | `refund_amount_inr` | float | Refund issued in ₹ |
| 43 | `warehouse` | str | Fulfilling warehouse location |
| 44 | `seller_id` | str | Seller identifier (SEL####) |
| 45 | `seller_rating` | float | Seller rating (3.0–5.0) |
| 46 | `product_rating` | float | Product rating (2.5–5.0) |
| 47 | `review_count` | int | Number of product reviews |
| 48 | `wishlist_flag` | int | 1 = Ordered from wishlist, 0 = Direct order |

---

## 🛠️ Tools & Libraries Used

| Library | Version | Purpose |
|---------|---------|---------|
| **Dask** | Latest | Parallel big data processing |
| **Pandas** | 1.5+ | Data manipulation and aggregation |
| **NumPy** | 1.23+ | Numerical operations |
| **Matplotlib** | 3.6+ | Charts and visualizations |
| **Seaborn** | 0.12+ | Statistical plots |

---

## 🔧 How to Run

### Step 1 — Install dependencies
```bash
pip install dask pandas numpy matplotlib seaborn pyarrow jupyter
```

### Step 2 — Clone or download this folder
```
Task-1-Big-Data-Analysis/
├── big_data_analysis.ipynb
├── ecommerce_big_data.csv
└── README.md
```

### Step 3 — Open the notebook
```bash
jupyter notebook big_data_analysis.ipynb
```

### Step 4 — Run all cells
**Kernel → Restart & Run All**

---

## 📓 Notebook Structure (43 Cells — 13 Sections)

| Section | Description |
|---------|-------------|
| 1 | Environment Setup & Library Imports |
| 2 | Load Dataset with Dask (Parallel Read) |
| 3 | Dataset Overview & Quality Check |
| 4 | KPI Dashboard (10 Key Metrics) |
| 5 | Sales & Revenue Analysis (Monthly + Quarterly Trend) |
| 6 | Category & Product Analysis (Revenue, Discounts, Top Brands) |
| 7 | Customer & Demographic Analysis (Segment, Age, Gender, Occupation) |
| 8 | Platform & Payment Analysis (8 Platforms, 8 Payment Methods) |
| 9 | Regional Analysis (15 States, 20 Cities, City Tiers) |
| 10 | Returns & Refund Analysis |
| 11 | Correlation & Advanced Analysis |
| 12 | Dask Scalability Demo (Dask vs Pandas benchmark) |
| 13 | Final Insights Summary |

---

## 📈 Key Business Insights

1. **50,000 orders** processed in parallel using Dask partitions
2. **UPI** is the most preferred payment method across all segments
3. **Tier 1 cities** contribute the highest revenue share
4. **Electronics** category generates maximum revenue
5. **Mobile devices** account for the majority of orders
6. **Amazon India & Flipkart** are the top revenue-generating platforms
7. Return rate is under **8%** — healthy for Indian e-commerce
8. **VIP & Premium customers** drive disproportionately high revenue
9. **25–34 age group** is the highest spending demographic
10. Dask processes this dataset **in parallel partitions** — the same code runs on 50M+ rows

---

## 🔑 Why Dask for Big Data?

| Feature | Pandas | Dask |
|---------|--------|------|
| Max Dataset Size | Limited by RAM | Larger than RAM ✅ |
| Processing | Single-core | Multi-core Parallel ✅ |
| API | Standard | Pandas-compatible ✅ |
| Lazy Evaluation | ❌ | ✅ |
| Scalability | Low | Cluster-scale ✅ |

---

<div align="center">
<b>CODTECH IT Solutions Pvt. Ltd.</b> — Data Analytics Internship — Task 1
</div>
