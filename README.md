# Fabric Report Load Balancer

An automated load balancing solution designed to optimize capacity utilization, protect critical reports, and reduce unnecessary scale-up costs.

This project dynamically repoints datasets between capacities based on configurable utilization thresholds — ensuring business-critical reports remain performant without increasing infrastructure spend.

This project originated from a recurring observation in the field:

“Our capacity became overloaded without us noticing, and it impacted our most important reports.”

While the first recommendation in such cases is often surge protection, there is still room for further optimization. This solution allows capacities to be more fully utilized while ensuring that critical reports continue to run smoothly.

---

> [!CAUTION]  
> This tool is in testing phase and we are mostly looking for feedback at the minute as there is quite a lot of hidden complexity, such as  TMDL copy of the semantic models,copy of refresh schedules, rebind of the reports, rebind of the datasources of the semantic model, etc.


## 🎯 Objectives

### 1. Prioritize Critical Reports

- Automatically prevent important reports from running under constrained capacity.
- When utilization approaches a defined threshold, datasets are **automatically moved** to a less utilized capacity.
- Ensures consistent report performance during peak loads.

---

### 2. Reduce Costs by Avoiding Unnecessary Scale-Ups

Instead of scaling up (and incurring additional cost):

- Datasets are moved to **underutilized capacities**.
- Running additional workloads on less utilized capacities incurs **zero additional cost**.
- Reports remain in the same workspace.
- No app repointing required.
- No user redirection.
- Seamless end-user experience.

---

### 3. Automatic Recovery (Rebind-Back)

- When the original (primary) capacity load returns to normal levels:
  - The dataset is automatically repointed back.
  - Recovery is governed by a configurable recovery threshold.
- Ensures temporary balancing without permanent architectural changes.

---

### 4. Flexible Deployment Frequency

- For a limited number of reports our advice is to schedule it each 10 minutes.
- Deploy and execute the process as frequently as needed.
- Fully configurable scheduling.
- Works with your operational cadence.

---

### 5. Fully Customizable Logic

The solution is extensible and fully adaptable:

- Integrate machine learning forecasting.
- Add seasonal load analysis.
- Implement custom prioritization logic.
- Modify utilization metrics (see below).
- Tailor behavior to your organization’s needs.

You are in full control of how the data is interpreted and acted upon.

---

### 6. Intelligent Alerting

If:
- There are reports that need to be repointed **and**
- Both the source and target capacities exceed threshold

Then:
- An alert record is written to a dedicated table in **Capacity Utilization Events**.
- You can trigger alerts using Fabric Activator.
- Send notifications to:
  - Microsoft Teams
  - Email
  - Any other preferred channel
 
- **This is not implementend - you will still need to create the activator and plug it into the events in the KQL Database**

This ensures no silent failures and full operational visibility.

---

### 7. Restart Behavior & Rebind Delay

In case of system restart:

- The rebind-back operation may be delayed.
- The solution considers **maximum consumption in the last 5 minutes**.
- Rebinding may take up to 5 minutes post-restart.
- This can be subject to change as we can look at the last timestamp on the events table.

Manual override:
- You can manually force a rebind.
- Simply delete the corresponding record from the `repoints` table.

---

### 8. Configurable Consumption Rules

Current default rule:
- Based on **overall capacity consumption**.

However, you can adapt it to:
- Interactive consumption only.
- Percentage of interactive load.
- Custom workload types.

The logic is entirely configurable.

---

### 9. Comparison to Surge Protection V1

This solution partially overlaps with Surge Protection V1.

| Surge Protection V1 | Capacity Load Balancer |
|----------------------|------------------------|
| Hard limit on % utilization for background jobs | No hard limit required so you can potentially fully utilize the capacity until throtthling and without affecting reports|
| Focused on limiting background activity | Focused on preserving critical reports |

Key differences:
- This solution looks at past consumption in the last 5 minutes which is different than surge protection which as it does not cancel running jobs you could still be in a bad position due to current jobs consumption.
- No need to enforce strict background consumption caps.
- Allows selective repointing of important datasets.
- Provides customizable alerting mechanisms.


### 10. This solution is mostly relevant for long running background consumption (which smooths over 24 hours)

Although this solution can help mitigate sudden spikes (from interactive consumption), due to notebook execution time it is most effective for managing slow-moving background consumption.

---

## 🏗️ High-Level Flow

1. Monitor capacity utilization.
2. Compare against defined threshold.
3. If threshold is exceeded:
   - Identify prioritized reports.
   - Repoint datasets to a less utilized capacity (if target capacity is below 0.8* threshold).
4. Monitor for recovery threshold.
5. Automatically rebind back when safe.
6. If no viable capacity exists:
   - Log event for alerting.

## 📊 Examples

The following scenarios demonstrate how the load balancer behaves with:

- **Move Threshold:** 84%  
- **Recovery Threshold:** 80%  

---

### 🚨 Scenario 1 — Capacity A Overloaded → Repoint to Capacity B

When:

- **Capacity A** reaches **85% utilization**  
- Move threshold is **84%**

Since **86% > 84% and Capacity B is under threshold 30% < 84%*0.8=~67 **, the system:

1. Creates a **TMDL copy** of the dataset.
2. Repoints the report to **Capacity B** (assuming B is below threshold).
3. Keeps the report in the same workspace (no user disruption).

**Result:** Load is shifted automatically to protect report performance.

<img width="487" height="450" alt="Capacity A overloaded - dataset repointed to Capacity B" src="https://github.com/user-attachments/assets/9e3de184-5be0-4b72-9c22-2c409c6d88e6" />

---

### 🔄 Scenario 2 — Recovery → Automatic Rebind Back

When:

- **Capacity A** drops to **79% utilization**
- Recovery threshold is **80%**

Since **79% < 80%**, the system:

1. Automatically rebinds the dataset back to **Capacity A**.
2. Restores the original configuration.
3. Maintains full transparency to end users.

**Result:** Temporary load balancing is reversed once the capacity stabilizes.

<img width="491" height="433" alt="Capacity A recovered - dataset rebound back" src="https://github.com/user-attachments/assets/241053a8-70e1-4b9c-9526-9bf1f1f937d3" />

---

### ⚠️ Scenario 3 — Both Capacities Over Threshold → Alert Triggered

When:

- **Capacity A > 84% or Capacity A > 80% (in case of being already moved)**
- **Capacity B > 84% * 0.8 **

Since no target capacity is available below threshold:

1. No repoint is performed.
2. An alert record is written to the **Capacity Utilization Events** table.
3. Fabric Activator (or equivalent) can notify:
   - Microsoft Teams
   - Email
   - Any configured channel

**Result:** Operational visibility without unsafe repointing.

<img width="490" height="424" alt="Both capacities overloaded - alert triggered" src="https://github.com/user-attachments/assets/7ec41fdf-67ba-4dac-87ef-2b43bb132c1d" />

---

## 💡 Installation

  - Run the notebook that is under /setup called fabric-Load-Balacing-setup.ipynb. Just change the capacity_name (capacity where it will reside) and workspace_name (name of your choice) to point to the right capacity.
---

## 💡 Configuration

- In the pipeline created LoadBalancingPipeline you will need to setup the below parameters:

- **clusterURI** – The Eventhouse query endpoint that contains the capacity events.

- **databaseName** – The database name of the KQL database. The table that is going to be read for checking events is hardcoded in the code inside `getCapacityConsumption`, by default it is called `CapacityEvents`.

- **reportIDs** – The report IDs (comma separated) that will be moved if the capacity gets overloaded.

- **TargetWorkspace** – If there is already a workspace where datasets should be copied (must be in another capacity).

- **WSName** – If there is no `TargetWorkspace`, the tool will create a new workspace with this name.

- **CapacityIdToDeploy** – Capacity where the workspace will be created.

- **CUusedPercentThreshold** – Threshold used to detect capacity overload.

- **RecoveryThreshold** – Must be `< CUusedPercentThreshold`.  
  Example: if the threshold is **85%** and recovery is **80%**, rebinding occurs at **85%** but revert only happens below **80%** to avoid frequent switching.
  
Obs: Make sure that the executing user or service principal has access to the underlying artifacts, such as: datasource connections and workspaces (source and target) and capacity events.

---

## 💡 Benefits

- Protects critical business reports
- Optimizes resource usage
- Reduces infrastructure cost
- Prevents performance degradation by avoiding interactive delays
- Fully customizable

---

## 🔧 Future Enhancements (Optional)

- Predictive capacity forecasting
- AI-driven workload classification
- Automated capacity selection optimization
- Advanced workload segmentation

---

## ⚠️ Important Notes

- There is a lakehouse which comes attached to the solution which contain two tables:
  1. Logs - contains logging for every run - so you will be able to see what operations were performed in each iteration.
  2. Report_repoints - a caching layer to observe what reports are currently repointed so they can be moved to the initial workspace in the future.
  
---

