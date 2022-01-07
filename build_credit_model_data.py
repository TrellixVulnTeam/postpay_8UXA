import ness
import numpy as np
import pandas as pd

from data_manager import (
    create_agg_var,
    create_unpaid_cols,
    days_from_today,
    last_row,
    partitioned_row_number,
    to_numeric,
    two_date_cols_diff,
)

# import datetime


# ### Create Unpaid ####


# Set the ness parameters
ness_parameters = {
    "bucket": "data.postpay.io",
    "key": "api",
    "profile": "default",
}
# Instantiate the data lake
dl = ness.dl(**ness_parameters)
# Read/Sync all the tables from the data lake
dl.sync()
# Create data frames from the tables
cart_df = dl.read("cart")
orders_df = dl.read("orders")
customers_df = dl.read("customers")
addresses_df = dl.read("addresses")
instalment_plans_df = dl.read("instalment-plans")
instalments_df = dl.read("instalments")
transactions_df = dl.read("transactions")
refunds_df = dl.read("refunds")


# Convert the anount columns to numeric
to_numeric(
    df=instalments_df,
    columns_list=["refunded_amount", "penalty_fee", "amount", "total"],
)
to_numeric(
    df=instalment_plans_df,
    columns_list=[
        "downpayment_amount",
        "total_downpayment",
        "total_amount",
        "shipping_amount",
        "downpayment_refunded_amount",
    ],
)


# Initial filters
pi3_bool = instalment_plans_df["num_instalments"] == 3
ae_bool = instalment_plans_df["payment_method_country"] == "AE"


# Create instalment_plans_df for ONLY "pi3" and "AE"
pi3_ae_instalment_plans_df = instalment_plans_df[pi3_bool & ae_bool]


# Create ID's table for ONLY "pi3" and "AE"
pi3_ae_instalment_plans_id_df = instalment_plans_df[pi3_bool & ae_bool][
    ["customer_id", "instalment_plan_id", "order_id"]
]


# Create instalments_df for ONLY "pi3" and "AE"
pi3_ae_instalments_df = pi3_ae_instalment_plans_id_df.merge(
    instalments_df, how="left", on="instalment_plan_id"
)


# Create filtered df by the last instalment per instalment_plan_id for ONLY
# "pi3" and "AE"
instalments_df_max_inst_number = last_row(
    df=pi3_ae_instalments_df,
    groupby_col="instalment_plan_id",
    by_last_col="order",
)


# Get instalments_df with only last instalment
instalments_df_last_inst = instalments_df_max_inst_number.merge(
    pi3_ae_instalments_df, how="left", on=["instalment_plan_id", "order"]
)


# Create colun with number of days between today and the scheduled date
instalments_df_last_inst_d = days_from_today(
    df=instalments_df_last_inst,
    new_col_name="days_since_scheduled",
    date_col="scheduled",
    drop_orig_col=False,
)


# Filter the df by mature data: only ('paid' or 'unpaid')
last_mature_pi3_ae_df = instalments_df_last_inst_d[
    (instalments_df_last_inst_d["status"].isin(["paid", "unpaid"]))
]


# unique instalment_plan_id for pi3 & AE & passed the scheduled date &
# only ('paid' or 'unpaid')
unique_id_pi3_ae_paid_unpaid = last_mature_pi3_ae_df["instalment_plan_id"].unique()


# Create the new instalments_df table with the entire instalments
# per instalment_plan_id
inst_pi3_ae_paid_unpaid_df = pi3_ae_instalments_df[
    pi3_ae_instalments_df["instalment_plan_id"].isin(unique_id_pi3_ae_paid_unpaid)
]


# Create unpaid variables per instalment_plan_id
unpaid_per_instalment_plan_id = create_agg_var(
    df=inst_pi3_ae_paid_unpaid_df,
    condition_column="status",
    condition_cat="unpaid",
    groupby_col="instalment_plan_id",
    orig_cols=["amount", "total"],
    new_col_names=["total_unpaid_amount", "total_unpaid_total"],
    agg_fnc="sum",
)


# # Create penalty variable per instalment_plan_id
# penalty_fees_per_instalment_plan_id = create_agg_var(
#     df=inst_pi3_ae_paid_unpaid_df,
#     groupby_col="instalment_plan_id",
#     orig_cols=["penalty_fee"],
#     new_col_names=["total_penalty_fee"],
#     agg_fnc="sum",
# )


# # Create refunds variable per order_id
# redunds_per_order_id = create_agg_var(
#     df=refunds_df,
#     groupby_col="order_id",
#     orig_cols=["amount"],
#     new_col_names=["total_refunds"],
#     agg_fnc="sum",
# )


# Create new days diff columns

# Scheduled-Complete Diff
inst_pi3_ae_paid_unpaid_d1_df = two_date_cols_diff(
    df=inst_pi3_ae_paid_unpaid_df,
    new_col_name="days_scheduled_completed",
    first_col="scheduled",
    second_col="completed",
)

# Scheduled-Today Diff
inst_pi3_ae_paid_unpaid_d2_df = days_from_today(
    df=inst_pi3_ae_paid_unpaid_d1_df,
    new_col_name="days_since_scheduled",
    date_col="scheduled",
)


# Create filtered df by the last instalment per instalment_plan_id
instalments_df_max_inst_number = last_row(
    df=inst_pi3_ae_paid_unpaid_d2_df,
    groupby_col="instalment_plan_id",
    by_last_col="order",
)


# Join instalments_df to instalments_df_max_inst_number
last_inst_df = pd.merge(
    instalments_df_max_inst_number,
    inst_pi3_ae_paid_unpaid_d2_df,
    how="left",
    on=["instalment_plan_id", "order"],
).rename(columns={"completed": "inst_completed_date", "status": "inst_status"})


# Join unpaid_per_instalment_plan_id to last_inst_df
last_inst_df_with_unpaid = last_inst_df.merge(
    unpaid_per_instalment_plan_id, how="left", on=["instalment_plan_id"]
)


col_to_keep = [
    "instalment_plan_id",
    "customer_id",
    "order_id",
    # 'inst_status',
    "days_scheduled_completed",
    "days_since_scheduled",
    "total_unpaid_amount",
    "total_unpaid_total",
    # "total_penalty_fee",
]
last_inst_df_with_unpaid = last_inst_df_with_unpaid[col_to_keep]


# ff
first_joined_postpay_df = (
    customers_df.groupby(by="customer_id")["created"]
    .min()
    .reset_index()
    .rename(columns={"created": "first_postpay_order"})
)


# # Get the first pi3 date
# first_pi3_order_df = (
#     pi3_ae_instalment_plans_df.groupby(by="customer_id")["created"]
#     .min()
#     .reset_index()
#     .rename(columns={"created": "first_pi3_order"})
# )


# Add first_postpay_order to the df
pi3_ae_instalment_plans_df = pi3_ae_instalment_plans_df.merge(
    first_joined_postpay_df, how="left", on="customer_id"
)


# Create is_returning variable
pi3_ae_instalment_plans_df["is_returning"] = np.where(
    (
        pi3_ae_instalment_plans_df["created"]
        - pi3_ae_instalment_plans_df["first_postpay_order"]
    ).dt.days
    < 1,
    False,
    True,
)


# Join the unpaid table to the instalment_plans_df
instalment_plans_unp_df = pd.merge(
    last_inst_df_with_unpaid,
    pi3_ae_instalment_plans_df[
        pi3_ae_instalment_plans_df.columns[
            ~pi3_ae_instalment_plans_df.columns.isin(
                ["order_id", "customer_id", "date_of_birth"]
            )
        ]
    ],
    how="left",
    on=["instalment_plan_id"],
).rename(
    columns={
        "completed": "inst_plan_completed_date",
        "status": "inst_plan_status",
    }
)


# # Join the refunds_df to the instalment_plans_unp_df
# instalment_plans_unp_ref_df = pd.merge(
#     instalment_plans_unp_df, redunds_per_order_id,
# how="left", on=["order_id"]
# )


# Create nr_of_items variable per order_id
cart_total_order_df = create_agg_var(
    df=cart_df,
    groupby_col="order_id",
    orig_cols=["qty"],
    new_col_names=["nr_of_items"],
    agg_fnc="sum",
)


# Join the cart_df to the instalment_plans_unp_df
instalment_plans_unp_ref_cart_df = pd.merge(
    instalment_plans_unp_df,
    cart_total_order_df,
    how="left",
    on=["order_id"],
)


# Recreate the unpaid status variable
instalment_plans_unp_ref_cart_df["unpaid_status"] = np.where(
    instalment_plans_unp_ref_cart_df["total_unpaid_amount"] > 0,
    "unpaid",
    "paid",
)


# Create the unpaid columns
create_unpaid_cols(
    df=instalment_plans_unp_ref_cart_df,
    unpaid_col_names=[
        "unpaid_at_due",
        "unpaid_at_5",
        "unpaid_at_10",
        "unpaid_at_20",
        "unpaid_at_30",
        "unpaid_at_60",
        "unpaid_at_90",
    ],
    condition_col="unpaid_status",
    col_to_check_paid="days_scheduled_completed",
    col_to_check_unpaid="days_since_scheduled",
    col_to_use_paid="total_amount",
    col_to_use_unpaid="total_unpaid_amount",
)


df_mature_at_due = instalment_plans_unp_ref_cart_df


# #############OLD#############

# # Create unpaid variables per instalment_plan_id
# unpaid_per_instalment_plan_id = create_agg_var(
#     df=instalments_df,
#     condition_column="status",
#     condition_cat="unpaid",
#     groupby_col="instalment_plan_id",
#     orig_cols=["amount", "total"],
#     new_col_names=["total_unpaid_amount", "total_unpaid_total"],
#     agg_fnc="sum",
# )


# # Create penalty variable per instalment_plan_id
# penalty_fees_per_instalment_plan_id = create_agg_var(
#     df=instalments_df,
#     groupby_col="instalment_plan_id",
#     orig_cols=["penalty_fee"],
#     new_col_names=["total_penalty_fee"],
#     agg_fnc="sum",
# )


# # Create new days diff columns

# # Scheduled-Complete Diff
# two_date_cols_diff(
#     df=instalments_df,
#     new_col_name="days_scheduled_completed",
#     first_col="scheduled",
#     second_col="completed",
# )

# # Scheduled-Today Diff
# days_from_today(
#     df=instalments_df,
#     new_col_name="days_since_scheduled",
#     date_col="scheduled",
# )


# # Create filtered df by the last instalment per instalment_plan_id
# instalments_df_max_inst_number = last_row(
#     df=instalments_df, groupby_col="instalment_plan_id", by_last_col="order"
# )


# # Join instalments_df to instalments_df_max_inst_number
# last_inst_df = pd.merge(
#     instalments_df_max_inst_number,
#     instalments_df,
#     how="left",
#     on=["instalment_plan_id", "order"],
# ).rename(columns={"completed": "inst_completed_date", "status": "inst_status"
# })


# # Join unpaid_per_instalment_plan_id to last_inst_df
# last_inst_df_with_unpaid = last_inst_df.merge(
#     unpaid_per_instalment_plan_id, how="left", on=["instalment_plan_id"]
# ).merge(
#     penalty_fees_per_instalment_plan_id, how="left", on=
# ["instalment_plan_id"]
# )


# # Join the unpaid table to the instalment_plans_df
# instalment_plans_unp_df = pd.merge(
#     instalment_plans_df,
#     last_inst_df_with_unpaid,
#     how="left",
#     on=["instalment_plan_id"],
# ).rename(
#     columns={
#         "completed": "inst_plan_completed_date",
#         "status": "inst_plan_status",
#     }
# )


# # Join the refunds_df to the instalment_plans_unp_df
# instalment_plans_unp_df = pd.merge(
#     instalment_plans_unp_df,
#     refunds_df[["order_id", "amount"]].rename(
#         columns={"amount": "refund_amount"}
#     ),
#     how="left",
#     on=["order_id"],
# )


# # Create nr_of_items variable per order_id
# cart_total_order_df = create_agg_var(
#     df=cart_df,
#     groupby_col="order_id",
#     orig_cols=["qty"],
#     new_col_names=["nr_of_items"],
#     agg_fnc="sum",
# )


# # Join the cart_df to the instalment_plans_unp_df
# instalment_plans_unp_df = pd.merge(
#     instalment_plans_unp_df, cart_total_order_df, how="left", on=["order_id"]
# )


# # Create the unpaid columns
# create_unpaid_cols(
#     df=instalment_plans_unp_df,
#     unpaid_col_names=[
#         "unpaid_at_due",
#         "unpaid_at_5",
#         "unpaid_at_10",
#         "unpaid_at_20",
#         "unpaid_at_30",
#         "unpaid_at_60",
#         "unpaid_at_90",
#     ],
#     condition_col="inst_status",
#     col_to_check_paid="days_scheduled_completed",
#     col_to_check_unpaid="days_since_scheduled",
#     col_to_use_paid="total_amount",
#     col_to_use_unpaid="total_unpaid_amount",
# )


# # Save a new df with mature (unpaid_at_60 is not null) orders
# df_mature_at_due = instalment_plans_unp_df[
#     pd.notnull(instalment_plans_unp_df["unpaid_at_due"])
# ]


# Keep only relevant variables (for my models)
columns_to_drop = [
    "payment_method_fingerprint",
    "downpayment_amount",
    "billing_address_id",
    "device_fingerprint",
    "total_downpayment",
    "checkout_completed",
    "merchant_id",
    "plan",
    "shipping_address_id",
    # "total_amount",
    "currency",
    "id_number",
    "checkout_verified",
    "cancelled",
    "shipping_amount",
    "reference",
    "inst_plan_completed_date",
    "payment_interval",
    "status_changed",
    "customer_email",
    "inst_plan_status",
    "shipping_id",
    "customer_blacklisted",
    "downpayment_refunded_amount",
    "phone",
    "ip_address",
    "transaction_cost_rate",
    "transaction_cost_amount",
    # "order",
    # "refunded_amount",
    # "penalty_fee",
    # "amount",
    # "scheduled",
    # "inst_completed_date",
    # "total",
    "days_scheduled_completed",
    # "days_since_scheduled",
    "total_unpaid_amount",
    "total_unpaid_total",
    "gateway_name",
    "payment_method_country",
    "first_postpay_order",
    "num_instalments",
    "unpaid_status",
    "customer_date_joined",
]
df_mature_at_due.drop(columns=columns_to_drop, inplace=True)


# ### Create Behaviour # ###

# Customers df #

# Filter customers_df to get only the relevant variables
filtered_customers_df = customers_df[
    ["customer_id", "created", "date_of_birth"]
].rename(columns={"created": "customer_first_joined"})


# AOV #

# Create temp AOV table
aov_tmp_df = pi3_ae_instalment_plans_df[
    ["instalment_plan_id", "customer_id", "created", "total_amount"]
]


# Add row number to the aov_tmp_df
aov_tmp_df["row_number"] = partitioned_row_number(
    df=aov_tmp_df, groupby_col_list=["customer_id"], sort_by_col_list=["created"]
)


# Sort aov_tmp_df
aov_tmp_df_sort = aov_tmp_df.sort_values(["customer_id", "row_number"])


# Create avg_order_value_list such that in each row it takes into account only previous orders
avg_order_value_list = []
for i in aov_tmp_df_sort["customer_id"].unique():
    for d in aov_tmp_df_sort[aov_tmp_df_sort["customer_id"] == i]["row_number"]:
        # AOV
        current_value = aov_tmp_df_sort[
            (aov_tmp_df_sort["customer_id"] == i) & (aov_tmp_df_sort["row_number"] < d)
        ]["total_amount"].mean()
        avg_order_value_list.append(current_value)


# Add avg_order_value to the df
aov_tmp_df_sort["avg_order_value"] = avg_order_value_list


# Fees #

# Create instalments_df for ONLY "pi3" and "AE"
pi3_ae_instalments_df = pi3_ae_instalment_plans_id_df.merge(
    instalments_df, how="left", on="instalment_plan_id"
)


# Create sum of fees per order
fees_df = create_agg_var(
    df=pi3_ae_instalments_df,
    groupby_col="instalment_plan_id",
    orig_cols=["penalty_fee"],
    new_col_names=["sum_fees_per_order"],
    agg_fnc="sum",
)

cid_fees_df = fees_df.merge(
    pi3_ae_instalment_plans_df[["instalment_plan_id", "customer_id", "created"]],
    how="left",
    on="instalment_plan_id",
)


# Add row number to the aov_tmp_df
cid_fees_df["row_number"] = partitioned_row_number(
    df=cid_fees_df, groupby_col_list=["customer_id"], sort_by_col_list=["created"]
)


# Sort cid_fees_df
cid_fees_df_sort = cid_fees_df.sort_values(["customer_id", "row_number"])


# Create fees_list per number of days past
fees_list = []
# cid_fees_df_sort['customer_id'].unique()
# Loop the customer_id
for i in cid_fees_df_sort["customer_id"].unique():
    # Loop the row_number per customer_id (which is the 'instalment_plan_id', but ordered)
    for d in cid_fees_df_sort[cid_fees_df_sort["customer_id"] == i]["row_number"]:
        # Define the conditions for row i nd d
        row_i = cid_fees_df_sort["customer_id"] == i
        row_i_d = (cid_fees_df_sort["customer_id"] == i) & (
            cid_fees_df_sort["row_number"] == d
        )
        # Get the date of the specific row
        current_day = cid_fees_df_sort[row_i_d]["created"].iloc[0]
        # Keep the previous orders
        hist_df = cid_fees_df_sort[row_i & (cid_fees_df_sort["row_number"] < d)]
        # Add current_day to the hist_df
        hist_df["current_day"] = current_day
        # Calculate the days diff between the 'current_day' and 'created' for each row in the hist_df
        hist_df["diff_days"] = (hist_df["current_day"] - hist_df["created"]).dt.days
        # Calculate the min date of the hist_df
        min_day = hist_df["created"].min()

        nr_days_list = [30, 90, 180, 365]
        tmp_list = []
        for y in nr_days_list:
            hist_df_y = hist_df[hist_df["diff_days"] < y]
            current_value = hist_df_y[row_i & (hist_df_y["row_number"] < d)][
                "sum_fees_per_order"
            ].mean()
            tmp_list.append(current_value)

        fees_list.append(tmp_list)


# Convert the fees_list to df
fees_df = pd.DataFrame(
    fees_list,
    columns=[
        "avg_fees_per_order_30d",
        "avg_fees_per_order_90d",
        "avg_fees_per_order_180d",
        "avg_fees_per_order_365d",
    ],
    dtype=float,
)


# Concat the fees_df with the main table
cid_avg_fees_df_sort = pd.concat(
    [cid_fees_df_sort.reset_index(drop=True), fees_df], axis=1
)


# Number of merchants #

# Create temp nr_merch table
nr_merch_tmp_df = pi3_ae_instalment_plans_df[
    ["instalment_plan_id", "customer_id", "created", "merchant_name"]
]


# Add row number to nr_merch_tmp_df
nr_merch_tmp_df["row_number"] = partitioned_row_number(
    df=nr_merch_tmp_df, groupby_col_list=["customer_id"], sort_by_col_list=["created"]
)


# Sort nr_merch_tmp_df
nr_merch_tmp_df_sort = nr_merch_tmp_df.sort_values(["customer_id", "row_number"])


# Create the nr_merchants_list
nr_merchants_list = []
for i in nr_merch_tmp_df_sort["customer_id"].unique():
    for d in nr_merch_tmp_df_sort[nr_merch_tmp_df_sort["customer_id"] == i][
        "row_number"
    ]:
        # Number of merchants
        current_value = nr_merch_tmp_df_sort[
            (nr_merch_tmp_df_sort["customer_id"] == i)
            & (nr_merch_tmp_df_sort["row_number"] < d)
        ]["merchant_name"].count()
        nr_merchants_list.append(current_value)


# Add count_merchants_per_customer to the df
nr_merch_tmp_df_sort["count_merchants_per_customer"] = nr_merchants_list


# Number of open orders #

# # Create df for due instalment
# cid_status_df = pi3_ae_instalment_plans_id_df[
#     ["instalment_plan_id", "customer_id"]
# ].merge(
#     instalments_df[["instalment_plan_id", "status", "total", "scheduled"]],
#     how="left",
#     on="instalment_plan_id",
# )
# cid_status_due_df = cid_status_df[cid_status_df["status"] == "due"]


# # Aggregate to get the last instalment scheduled
# due_max_schedule_df = create_agg_var(
#     df=cid_status_due_df,
#     groupby_col="instalment_plan_id",
#     orig_cols=["scheduled"],
#     new_col_names=["max_scheduled"],
#     agg_fnc="max",
# )


# # Merge the df of last due instalment with the rest of due instalments
# due_max_schedule_inst_df = due_max_schedule_df.merge(
#     cid_status_due_df[["instalment_plan_id", "scheduled", "customer_id"]],
#     how="left",
#     left_on=["instalment_plan_id", "max_scheduled"],
#     right_on=["instalment_plan_id", "scheduled"],
# )


# # Add row number to due_max_schedule_inst_df
# due_max_schedule_inst_df["row_number"] = partitioned_row_number(
#     df=due_max_schedule_inst_df,
#     groupby_col_list=["customer_id"],
#     sort_by_col_list=["scheduled"],
# )


# # Sort due_max_schedule_inst_df
# due_max_schedule_inst_df_sort = due_max_schedule_inst_df.sort_values(
#     ["customer_id", "row_number"]
# )


# # Create nr_open_orders_list
# nr_open_orders_list = []
# for i in due_max_schedule_inst_df_sort["customer_id"].unique():
#     for d in due_max_schedule_inst_df_sort[
#         due_max_schedule_inst_df_sort["customer_id"] == i
#     ]["row_number"]:
#         # Number of merchants
#         current_value = due_max_schedule_inst_df_sort[
#             (due_max_schedule_inst_df_sort["customer_id"] == i)
#             & (due_max_schedule_inst_df_sort["row_number"] < d)
#         ]["customer_id"].count()
#         nr_open_orders_list.append(current_value)


# # Add count_open_orders to the df
# due_max_schedule_inst_df_sort["count_open_orders"] = nr_open_orders_list


# # Number of paid instalments #

# # Create paid df
# inst_status_df = pi3_ae_instalment_plans_df[
#     ["instalment_plan_id", "customer_id", "created"]
# ].merge(
#     instalments_df[["instalment_plan_id", "status", "total", "scheduled"]],
#     how="left",
#     on="instalment_plan_id",
# )
# inst_status_paid_df = inst_status_df[inst_status_df["status"] == "paid"]


# # Merge the paid df with pi3-ae
# paid_inst_plan_df = pi3_ae_instalment_plans_df[
#     ["instalment_plan_id", "customer_id", "created"]
# ].merge(
#     inst_status_paid_df[["instalment_plan_id", "status"]]
#     .groupby(by="instalment_plan_id")
#     .count()
#     .reset_index(),
#     how="left",
#     on="instalment_plan_id",
# )


# # Fill the na's with 0
# paid_inst_plan_df.fillna(0, inplace=True)


# # Add row number
# paid_inst_plan_df["row_number"] = partitioned_row_number(
#     df=paid_inst_plan_df, groupby_col_list=["customer_id"], sort_by_col_list=["created"]
# )


# # Sort paid_inst_plan_df
# paid_inst_plan_df_sort = paid_inst_plan_df.sort_values(["customer_id", "row_number"])


# # Create nr_paid_inst_list
# nr_paid_inst_list = []
# for i in paid_inst_plan_df_sort["customer_id"].unique():
#     for d in paid_inst_plan_df_sort[paid_inst_plan_df_sort["customer_id"] == i][
#         "row_number"
#     ]:
#         # Number of merchants
#         current_value = paid_inst_plan_df_sort[
#             (paid_inst_plan_df_sort["customer_id"] == i)
#             & (paid_inst_plan_df_sort["row_number"] < d)
#         ]["status"].sum()
#         nr_paid_inst_list.append(current_value)


# # Add count_paid_instalments to the df
# paid_inst_plan_df_sort["count_paid_instalments"] = nr_paid_inst_list


# # Number of unpaid instalments #

# # Create tmp unpaid
# inst_status_df = pi3_ae_instalment_plans_df[
#     ["instalment_plan_id", "customer_id", "created"]
# ].merge(
#     instalments_df[["instalment_plan_id", "status", "total", "scheduled"]],
#     how="left",
#     on="instalment_plan_id",
# )
# inst_status_unpaid_df = inst_status_df[inst_status_df["status"] == "unpaid"]


# # Merge to get the instalment plan with the instalments
# unp_inst_plan_df = pi3_ae_instalment_plans_df[
#     ["instalment_plan_id", "customer_id", "created"]
# ].merge(
#     inst_status_unpaid_df[["instalment_plan_id", "status"]]
#     .groupby(by="instalment_plan_id")
#     .count()
#     .reset_index(),
#     how="left",
#     on="instalment_plan_id",
# )


# # Replace na's with 0
# unp_inst_plan_df.fillna(0, inplace=True)


# # Add row number
# unp_inst_plan_df["row_number"] = partitioned_row_number(
#     df=unp_inst_plan_df, groupby_col_list=["customer_id"], sort_by_col_list=["created"]
# )


# # Sort unp_inst_plan_df
# unp_inst_plan_df_sort = unp_inst_plan_df.sort_values(["customer_id", "row_number"])


# # Create the nr_unpaid_inst_list
# nr_unpaid_inst_list = []
# for i in unp_inst_plan_df_sort["customer_id"].unique():
#     for d in unp_inst_plan_df_sort[unp_inst_plan_df_sort["customer_id"] == i][
#         "row_number"
#     ]:
#         # Number of merchants
#         current_value = unp_inst_plan_df_sort[
#             (unp_inst_plan_df_sort["customer_id"] == i)
#             & (unp_inst_plan_df_sort["row_number"] < d)
#         ]["status"].sum()
#         nr_unpaid_inst_list.append(current_value)


# # Add count_unpaid_instalments to the df
# unp_inst_plan_df_sort["count_unpaid_instalments"] = nr_unpaid_inst_list


# # Number of paid orders #

# # Create not-paid df
# not_paid_status_df = instalments_df.merge(
#     instalments_df[~instalments_df["status"].isin(["paid"])][
#         ["instalment_plan_id", "status"]
#     ]
#     .groupby(by="instalment_plan_id")
#     .min()
#     .reset_index()
#     .rename(columns={"status": "not_paid_status"}),
#     how="left",
#     on="instalment_plan_id",
# )


# # Keep only fully paid orders
# fully_paid_df = not_paid_status_df[
#     (not_paid_status_df["status"] == "paid")
#     & (pd.isnull(not_paid_status_df["not_paid_status"]))
# ]


# # Aggregate by instalment_plan_id
# fully_paid_inst_df = (
#     fully_paid_df[["instalment_plan_id", "status"]]
#     .groupby(by="instalment_plan_id")
#     .min()
#     .reset_index()
# )


# # Create the paid df
# order_status_df = pi3_ae_instalment_plans_df[
#     ["instalment_plan_id", "customer_id", "created"]
# ].merge(fully_paid_inst_df, how="left", on="instalment_plan_id")
# order_status_paid_df = order_status_df[order_status_df["status"] == "paid"]


# # Add row number
# order_status_paid_df["row_number"] = partitioned_row_number(
#     df=order_status_paid_df,
#     groupby_col_list=["customer_id"],
#     sort_by_col_list=["created"],
# )


# # Sort order_status_paid_df
# order_status_paid_df_sort = order_status_paid_df.sort_values(
#     ["customer_id", "row_number"]
# )


# # Create the nr_paid_orders_list
# nr_paid_orders_list = []
# for i in order_status_paid_df_sort["customer_id"].unique():
#     for d in order_status_paid_df_sort[order_status_paid_df_sort["customer_id"] == i][
#         "row_number"
#     ]:
#         # Number of merchants
#         current_value = order_status_paid_df_sort[
#             (order_status_paid_df_sort["customer_id"] == i)
#             & (order_status_paid_df_sort["row_number"] < d)
#         ]["customer_id"].count()
#         nr_paid_orders_list.append(current_value)


# # Add count_paid_orders to the df
# order_status_paid_df_sort["count_paid_orders"] = nr_paid_orders_list


# # Number of unpaid orders #

# # Get the unpaid status per instalment_plan_id
# unpaid_status_df = (
#     instalments_df[instalments_df["status"] == "unpaid"][
#         ["instalment_plan_id", "status"]
#     ]
#     .groupby(by="instalment_plan_id")
#     .min()
#     .reset_index()
#     .rename(columns={"status": "unpaid_status"})
# )


# # Get the unpaid df
# order_status_unp_df = pi3_ae_instalment_plans_df[
#     ["instalment_plan_id", "customer_id", "created"]
# ].merge(unpaid_status_df, how="left", on="instalment_plan_id")
# order_status_unpaid_df = order_status_unp_df[
#     order_status_unp_df["unpaid_status"] == "unpaid"
# ]


# # Add row number
# order_status_unpaid_df["row_number"] = partitioned_row_number(
#     df=order_status_unpaid_df,
#     groupby_col_list=["customer_id"],
#     sort_by_col_list=["created"],
# )


# # Sort order_status_unpaid_df
# order_status_unpaid_df_sort = order_status_unpaid_df.sort_values(
#     ["customer_id", "row_number"]
# )


# # Create the nr_unpaid_orders_list
# nr_unpaid_orders_list = []
# for i in order_status_unpaid_df_sort["customer_id"].unique():
#     for d in order_status_unpaid_df_sort[
#         order_status_unpaid_df_sort["customer_id"] == i
#     ]["row_number"]:
#         # Number of merchants
#         current_value = order_status_unpaid_df_sort[
#             (order_status_unpaid_df_sort["customer_id"] == i)
#             & (order_status_unpaid_df_sort["row_number"] < d)
#         ]["customer_id"].count()
#         nr_unpaid_orders_list.append(current_value)


# # Add count_unpaid_orders to the df
# order_status_unpaid_df_sort["count_unpaid_orders"] = nr_unpaid_orders_list


# The sum of outstanding captured debt #

# Create the 'due' df
cid_status_df = pi3_ae_instalment_plans_df[["instalment_plan_id", "customer_id"]].merge(
    instalments_df[["instalment_plan_id", "status", "total"]],
    how="left",
    on="instalment_plan_id",
)
cid_status_due_df = cid_status_df[cid_status_df["status"] == "due"]


# Aggregate by instalment_plan_id
inst_out_df = (
    cid_status_due_df[["instalment_plan_id", "total"]]
    .groupby(by="instalment_plan_id")
    .sum()
    .reset_index()
    .rename(columns={"total": "total_outstanding_debt"})
)


# Merge to get the instalment plan for outstanding orders
inst_plan_out_df = pi3_ae_instalment_plans_df[
    ["instalment_plan_id", "customer_id", "created"]
].merge(inst_out_df, how="left", on="instalment_plan_id")


# Add row number
inst_plan_out_df["row_number"] = partitioned_row_number(
    df=inst_plan_out_df, groupby_col_list=["customer_id"], sort_by_col_list=["created"]
)


# Sort inst_plan_out_df
inst_plan_out_df_sort = inst_plan_out_df.sort_values(["customer_id", "row_number"])


# Create the sum_outstanding_debt_list
sum_outstanding_debt_list = []
for i in inst_plan_out_df_sort["customer_id"].unique():
    for d in inst_plan_out_df_sort[inst_plan_out_df_sort["customer_id"] == i][
        "row_number"
    ]:
        # Number of merchants
        current_value = inst_plan_out_df_sort[
            (inst_plan_out_df_sort["customer_id"] == i)
            & (inst_plan_out_df_sort["row_number"] < d)
        ]["total_outstanding_debt"].sum()
        sum_outstanding_debt_list.append(current_value)


# Add current_exposure to the df
inst_plan_out_df_sort["current_exposure"] = sum_outstanding_debt_list


# The total order sum amount #

# Create the not paid status df
not_paid_status_df = instalments_df.merge(
    instalments_df[~instalments_df["status"].isin(["paid"])][
        ["instalment_plan_id", "status"]
    ]
    .groupby(by="instalment_plan_id")
    .min()
    .reset_index()
    .rename(columns={"status": "not_paid_status"}),
    how="left",
    on="instalment_plan_id",
)


# Keep only fully paid orders
fully_paid_df = not_paid_status_df[
    (not_paid_status_df["status"] == "paid")
    & (pd.isnull(not_paid_status_df["not_paid_status"]))
]


# Aggregate by instalment_plan_id
fully_paid_inst_df = (
    fully_paid_df[["instalment_plan_id", "status"]]
    .groupby(by="instalment_plan_id")
    .min()
    .reset_index()
)


# Merge to get the order status df
order_status_df = pi3_ae_instalment_plans_df[
    ["instalment_plan_id", "customer_id", "created", "total_amount"]
].merge(fully_paid_inst_df, how="left", on="instalment_plan_id")
order_status_paid_df_sum = order_status_df[order_status_df["status"] == "paid"]


# Add row number
order_status_paid_df_sum["row_number"] = partitioned_row_number(
    df=order_status_paid_df_sum,
    groupby_col_list=["customer_id"],
    sort_by_col_list=["created"],
)


# Sort order_status_paid_df_sum
order_status_paid_df_sum_sort = order_status_paid_df_sum.sort_values(
    ["customer_id", "row_number"]
)


# Create the sum_paid_orders_list
sum_paid_orders_list = []
for i in order_status_paid_df_sum_sort["customer_id"].unique():
    for d in order_status_paid_df_sum_sort[
        order_status_paid_df_sum_sort["customer_id"] == i
    ]["row_number"]:
        # Number of merchants
        current_value = order_status_paid_df_sum_sort[
            (order_status_paid_df_sum_sort["customer_id"] == i)
            & (order_status_paid_df_sum_sort["row_number"] < d)
        ]["total_amount"].sum()
        sum_paid_orders_list.append(current_value)


# Add sum_paid_amount to the df
order_status_paid_df_sum_sort["sum_paid_amount"] = sum_paid_orders_list


# # Days since last unpaid

# # Get the unpaid df
# cid_scheduled_status_df = pi3_ae_instalment_plans_df.loc[
#     :, ["instalment_plan_id", "customer_id"]
# ].merge(
#     instalments_df.loc[:, ["instalment_plan_id", "scheduled", "status", "total"]],
#     how="left",
#     on="instalment_plan_id",
# )
# cid_scheduled_status_unpaid_df = cid_scheduled_status_df.loc[
#     cid_status_df.loc[:, "status"] == "unpaid"
# ]


# # Get the last unpaid per instalment_plan_id
# last_unpaid_df = (
#     cid_scheduled_status_unpaid_df.groupby(by="instalment_plan_id")["scheduled"]
#     .max()
#     .reset_index()
# )


# # Keep only the last unpaid instalment
# cid_scheduled_status_last_unpaid_df = last_unpaid_df.merge(
#     cid_scheduled_status_unpaid_df, how="left", on=["instalment_plan_id", "scheduled"]
# )


# # Add row number
# cid_scheduled_status_last_unpaid_df.loc[:, "row_number"] = partitioned_row_number(
#     df=cid_scheduled_status_last_unpaid_df,
#     groupby_col_list=["customer_id"],
#     sort_by_col_list=["scheduled"],
# )


# # Sort cid_scheduled_status_last_unpaid_df
# cid_scheduled_status_last_unpaid_df_sort = (
#     cid_scheduled_status_last_unpaid_df.sort_values(["customer_id", "row_number"])
# )


# # Create lag for 'schedule' column
# cid_scheduled_status_last_unpaid_df_sort.loc[
#     :, "scheduled_lag"
# ] = cid_scheduled_status_last_unpaid_df_sort.groupby(by="customer_id")[
#     "scheduled"
# ].shift(
#     1
# )


# # Calculate the number od days between today and last unpaid date
# days_since_last_unpaid_df = days_from_today(
#     df=cid_scheduled_status_last_unpaid_df_sort,
#     new_col_name="days_since_last_unpaid",
#     date_col="scheduled_lag",
#     drop_orig_col=True,
# )

# MERGE #

# Merge all behaviour to one table
behavioural_instalment_plan_df = (
    pi3_ae_instalment_plans_id_df.merge(
        aov_tmp_df_sort[
            ["instalment_plan_id", "created", "total_amount", "avg_order_value"]
        ],
        how="left",
        on="instalment_plan_id",
    )
    .merge(
        cid_avg_fees_df_sort[
            [
                "instalment_plan_id",
                "avg_fees_per_order_30d",
                "avg_fees_per_order_90d",
                "avg_fees_per_order_180d",
                "avg_fees_per_order_365d",
            ]
        ],
        how="left",
        on="instalment_plan_id",
    )
    .merge(
        nr_merch_tmp_df_sort[["instalment_plan_id", "count_merchants_per_customer"]],
        how="left",
        on="instalment_plan_id",
    )
    # .merge(
    #     due_max_schedule_inst_df_sort[["instalment_plan_id", "count_open_orders"]],
    #     how="left",
    #     on="instalment_plan_id",
    # )
    # .merge(
    #     paid_inst_plan_df_sort[["instalment_plan_id", "count_paid_instalments"]],
    #     how="left",
    #     on="instalment_plan_id",
    # )
    # .merge(
    #     unp_inst_plan_df_sort[["instalment_plan_id", "count_unpaid_instalments"]],
    #     how="left",
    #     on="instalment_plan_id",
    # )
    # .merge(
    #     order_status_paid_df_sort[["instalment_plan_id", "count_paid_orders"]],
    #     how="left",
    #     on="instalment_plan_id",
    # )
    # .merge(
    #     order_status_unpaid_df_sort[["instalment_plan_id", "count_unpaid_orders"]],
    #     how="left",
    #     on="instalment_plan_id",
    # )
    .merge(
        inst_plan_out_df_sort[["instalment_plan_id", "current_exposure"]],
        how="left",
        on="instalment_plan_id",
    )
    .merge(
        order_status_paid_df_sum_sort[["instalment_plan_id", "sum_paid_amount"]],
        how="left",
        on="instalment_plan_id",
    )
    .merge(
        filtered_customers_df[
            ["customer_id", "customer_first_joined", "date_of_birth"]
        ],
        how="left",
        on="customer_id",
    )
    # .merge(
    #     days_since_last_unpaid_df[["instalment_plan_id", "days_since_last_unpaid"]],
    #     how="left",
    #     on="instalment_plan_id",
    # )
)


# ### Merge unpaid with behavioural table ### #
behaviour_mature_at_due_df = df_mature_at_due.drop(columns=["total_amount"]).merge(
    behavioural_instalment_plan_df.drop(columns=["customer_id", "order_id", "created"]),
    how="left",
    on=["instalment_plan_id"],
)


# Write to CSV
behaviour_mature_at_due_df.to_csv(
    "/Users/ronsnir/Documents/postpay/data/behaviour_mature_at_due_df_v3.csv",
    index=False,
)
