import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import json, os
import numpy as np

OUT  = "/tmp/charts_v19"
BASE = "/sessions/fervent-eager-turing/mnt/UNICAL_Agents/Tests/comparison_results"
os.makedirs(OUT, exist_ok=True)

# ── Bigger fonts for double-column paper ────────────────────────────────────
TITLE_FS=26; LABEL_FS=23; TICK_FS=21; LEGEND_FS=20; ANNOT_FS=20
mpl.rcParams.update({
    "axes.titlesize":TITLE_FS,"axes.titleweight":"bold","axes.labelsize":LABEL_FS,
    "axes.labelweight":"bold","xtick.labelsize":TICK_FS,"ytick.labelsize":TICK_FS,
    "legend.fontsize":LEGEND_FS,"font.size":TICK_FS,"font.weight":"bold",
    "lines.linewidth":2.5,"figure.dpi":100,
})

COLORS={"passive":"#9E9E9E","threshold":"#1976D2","weights":"#F57C00","ml":"#388E3C"}
LABELS={"passive":"Passive","threshold":"Threshold","weights":"Weights","ml":"ML"}
RUN_COLORS=["#64B5F6","#1976D2","#0D47A1"]
RUN_LABELS=["Run 1","Run 2","Run 3"]
POLICIES=["passive","threshold","weights","ml"]
TOTAL_S = 719 * 5

csv_files={"Run 1":f"{BASE}/comparison_20260516_1607.csv",
           "Run 2":f"{BASE}/comparison_20260516_2236.csv",
           "Run 3":f"{BASE}/comparison_20260517_0247.csv"}
stats_files={"Run 1":f"{BASE}/stats_20260516_1607.json",
             "Run 2":f"{BASE}/stats_20260516_2236.json",
             "Run 3":f"{BASE}/stats_20260517_0247.json"}

dfs={run:pd.read_csv(p) for run,p in csv_files.items()}
all_stats={}
for run,p in stats_files.items():
    with open(p) as f: all_stats[run]=json.load(f)

def avg_stat(key,policy):
    vals=[all_stats[r][policy][key] for r in RUN_LABELS
          if policy in all_stats[r] and all_stats[r][policy].get(key) is not None]
    return sum(vals)/len(vals) if vals else 0

def get_stat(run,policy,key,default=0):
    if policy in all_stats[run] and all_stats[run][policy].get(key) is not None:
        return all_stats[run][policy][key]
    return default


# ── Most representative run (min normalized distance from per-policy mean) ──
_REP_METRICS=["mean_qos","pct_below_sla","n_violations","avg_recovery_s","avg_qos_burn"]
_rep_scores={rn:0.0 for rn in RUN_LABELS}
for mk in _REP_METRICS:
    for p in POLICIES:
        _mean_p=avg_stat(mk,p)
        _std_p=np.std([get_stat(r,p,mk) for r in RUN_LABELS],ddof=1)
        for rn in RUN_LABELS:
            if _std_p>0:
                _rep_scores[rn]+=abs(get_stat(rn,p,mk)-_mean_p)/_std_p
REP_RUN=min(_rep_scores,key=_rep_scores.get)
print(f"Representative run: {REP_RUN}  scores={_rep_scores}")

def save(name):
    plt.savefig(f"{OUT}/{name}.pdf",format="pdf",bbox_inches="tight")
    plt.close()
    print(name+" done")

def bar_label(ax, bar, text, ylim_top, threshold=0.45):
    """Inside bar (white) if tall, else just above (dark)."""
    h = bar.get_height()
    if h == 0: return
    if h / ylim_top >= threshold:
        ax.text(bar.get_x()+bar.get_width()/2, h-ylim_top*0.04,
                text, ha="center", va="top", fontsize=ANNOT_FS,
                fontweight="bold", color="white", zorder=5)
    else:
        ax.text(bar.get_x()+bar.get_width()/2, h+ylim_top*0.012,
                text, ha="center", va="bottom", fontsize=ANNOT_FS,
                fontweight="bold", color="#111111", zorder=5)

def bar_label_top(ax, bar, text, ylim_top):
    """Always black, always above the bar — for violation counts."""
    h = bar.get_height()
    if h == 0: return
    ax.text(bar.get_x()+bar.get_width()/2, h + ylim_top*0.018,
            text, ha="center", va="bottom", fontsize=ANNOT_FS,
            fontweight="bold", color="#111111", zorder=5)

# zigzag multipliers: Run1 low, Run2 high, Run3 low  (pyramid pattern)
_ZZ_MULT = [1.10, 1.55, 1.10]   # used by log-scale figures
_ZZ_LIN  = [0.0,  0.14, 0.0]    # fraction of ylim added; used by linear figures

def bar_label_smart(ax, bar, text, ylim_top, run_idx=0):
    """Inside white for tall bars; zigzag black outside for short bars (linear scale)."""
    h = bar.get_height()
    if h == 0: return
    if h / ylim_top >= 0.32:
        ax.text(bar.get_x()+bar.get_width()/2, h - ylim_top*0.04,
                text, ha="center", va="top", fontsize=ANNOT_FS,
                fontweight="bold", color="white", zorder=5)
    else:
        offset = ylim_top * (0.016 + _ZZ_LIN[run_idx % 3])
        ax.text(bar.get_x()+bar.get_width()/2, h + offset,
                text, ha="center", va="bottom", fontsize=ANNOT_FS,
                fontweight="bold", color="#111111", zorder=5)

def bar_label_log(ax, bar, text, run_idx=0):
    """For log-scale axes: labels above bar with zigzag multiplicative offset."""
    h = bar.get_height()
    if h == 0: return
    y = h * _ZZ_MULT[run_idx % 3]
    ax.text(bar.get_x()+bar.get_width()/2, y,
            text, ha="center", va="bottom", fontsize=ANNOT_FS,
            fontweight="bold", color="#111111", zorder=5)


# ===========================================================================
# FIG 1 - QoS time series 2x2  (Run 2)
# ===========================================================================
fig, axes = plt.subplots(2,2,figsize=(18,11),sharex=True,sharey=True)
fig.suptitle("QoS Over Time - Run 2",fontsize=TITLE_FS+2,fontweight="bold",y=1.01)
df2=dfs["Run 2"]
for ax,policy in zip(axes.flatten(),POLICIES):
    sub=df2[df2["policy"]==policy].copy()
    ax.fill_between(sub["elapsed_s"]/60,sub["qos"],alpha=0.15,color=COLORS[policy])
    ax.plot(sub["elapsed_s"]/60,sub["qos"],color=COLORS[policy],linewidth=2.0,label=LABELS[policy])
    ax.set_ylim(0,108); ax.set_yticks([0,25,50,70,100])
    ax.legend(loc="lower right",fontsize=LEGEND_FS)
    ax.set_title(LABELS[policy],fontsize=TITLE_FS,loc="left",pad=4)
    ax.grid(True,alpha=0.3)
for ax in axes[1]: ax.set_xlabel("Time (minutes)")
for ax in axes[:,0]: ax.set_ylabel("QoS (0-100)")
plt.tight_layout(); save("fig1_qos_timeseries")

_F1_TITLE=TITLE_FS+16; _F1_LABEL=LABEL_FS+14; _F1_TICK=TICK_FS+10; _F1_LEG=LEGEND_FS+8
for policy in POLICIES:
    fig,ax=plt.subplots(figsize=(24,11))
    sub=df2[df2["policy"]==policy].copy()
    ax.fill_between(sub["elapsed_s"]/60,sub["qos"],alpha=0.15,color=COLORS[policy])
    ax.plot(sub["elapsed_s"]/60,sub["qos"],color=COLORS[policy],linewidth=3.0,label=LABELS[policy])
    ax.axhline(70,color="#555555",linestyle="--",linewidth=2.0,alpha=0.75,label="SLA threshold (QoS 70)")
    ax.set_ylim(0,108); ax.set_yticks([0,25,50,75,100])
    ax.set_xlabel("Time (minutes)",fontsize=_F1_LABEL,fontweight="bold")
    ax.set_ylabel("QoS",fontsize=_F1_LABEL,fontweight="bold")
    ax.set_title(f"QoS Over Time — {LABELS[policy]}",fontweight="bold",fontsize=_F1_TITLE)
    ax.tick_params(axis="both",labelsize=_F1_TICK,width=2,length=6)
    ax.legend(loc="lower right",fontsize=_F1_LEG,framealpha=0.9)
    ax.grid(True,alpha=0.3)
    plt.tight_layout(); save(f"fig1_{policy}")

# ===========================================================================
# FIG 2 - Burn vs Idle
# ===========================================================================
fig,ax=plt.subplots(figsize=(14,7))
x=np.arange(len(POLICIES)); w=0.35
burn_vals=[avg_stat("avg_qos_burn",p) for p in POLICIES]
idle_vals=[]
for p in POLICIES:
    vals=[all_stats[r][p]["avg_qos_idle"] for r in RUN_LABELS
          if p in all_stats[r] and all_stats[r][p].get("avg_qos_idle") is not None]
    idle_vals.append(sum(vals)/len(vals) if vals else 0)

bars1=ax.bar(x-w/2,burn_vals,w,label="CPU Burn Phase",color=[COLORS[p] for p in POLICIES],alpha=0.85)
bars2=ax.bar(x+w/2,idle_vals,w,label="CPU Idle Phase",color=[COLORS[p] for p in POLICIES],alpha=0.45,hatch="//")
ax.set_xticks(x); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Average QoS (0-100)"); ax.set_title("Average QoS: CPU Burn vs. Idle Phase",fontweight="bold")
ax.legend(fontsize=LEGEND_FS); ax.grid(True,axis="y",alpha=0.3); ax.set_ylim(0,118)
for bar in bars1:
    bar_label_top(ax,bar,f"{bar.get_height():.1f}",118)
for bar in bars2:
    bar_label_top(ax,bar,f"{bar.get_height():.1f}",118)
plt.tight_layout(); save("fig2_burn_vs_idle")

# ===========================================================================
# FIG 3 - SLA violations (combined + singles) — labels always black on top
# ===========================================================================
pct_vals=[avg_stat("pct_below_sla",p) for p in POLICIES]
viol_vals=[avg_stat("n_violations",p) for p in POLICIES]
bar_colors=[COLORS[p] for p in POLICIES]
xlabels=[LABELS[p] for p in POLICIES]

fig,(ax1,ax2)=plt.subplots(1,2,figsize=(16,7))
fig.suptitle("SLA Compliance Metrics",fontsize=TITLE_FS+1,fontweight="bold")
bars=ax1.bar(xlabels,pct_vals,color=bar_colors,alpha=0.85,edgecolor="white",linewidth=1.5)
ax1.set_ylabel("% of time below QoS 70"); ax1.set_title("% Time Below SLA Threshold",fontweight="bold")
ax1.set_ylim(0,66); ax1.grid(True,axis="y",alpha=0.3)
for bar,v in zip(bars,pct_vals): bar_label_top(ax1,bar,f"{v:.1f}%",66)
bars2=ax2.bar(xlabels,viol_vals,color=bar_colors,alpha=0.85,edgecolor="white",linewidth=1.5)
ax2.set_ylabel("SLA violation events"); ax2.set_title("SLA Violation Events",fontweight="bold")
ax2.set_ylim(0,11); ax2.grid(True,axis="y",alpha=0.3); ax2.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
for bar,v in zip(bars2,viol_vals): bar_label_top(ax2,bar,f"{v:.1f}",11)
plt.tight_layout(); save("fig3_sla_violations")

fig,ax=plt.subplots(figsize=(13,7))
bars=ax.bar(xlabels,pct_vals,color=bar_colors,alpha=0.85,edgecolor="white",linewidth=1.5)
ax.set_ylabel("% of time below QoS 70"); ax.set_title("% Time Below SLA Threshold",fontweight="bold")
ax.set_ylim(0,66); ax.grid(True,axis="y",alpha=0.3)
for bar,v in zip(bars,pct_vals): bar_label_top(ax,bar,f"{v:.1f}%",66)
plt.tight_layout(); save("fig3a_pct_below_sla")

fig,ax=plt.subplots(figsize=(13,7))
bars=ax.bar(xlabels,viol_vals,color=bar_colors,alpha=0.85,edgecolor="white",linewidth=1.5)
ax.set_ylabel("SLA violation events"); ax.set_title("SLA Violation Events",fontweight="bold")
ax.set_ylim(0,11); ax.grid(True,axis="y",alpha=0.3); ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
for bar,v in zip(bars,viol_vals): bar_label_top(ax,bar,f"{v:.1f}",11)
plt.tight_layout(); save("fig3b_sla_events")

# ===========================================================================
# FIG 4 - Recovery time
# ===========================================================================
fig,ax=plt.subplots(figsize=(13,7))
rec_vals=[avg_stat("avg_recovery_s",p) for p in POLICIES]
bars=ax.barh([LABELS[p] for p in POLICIES],rec_vals,color=[COLORS[p] for p in POLICIES],
             alpha=0.85,edgecolor="white",linewidth=1.5)
ax.set_xlabel("Average Recovery Time (seconds)"); ax.set_title("Average QoS Recovery Time After CPU Burn Onset",fontweight="bold")
ax.set_xlim(0,370); ax.grid(True,axis="x",alpha=0.3); ax.invert_yaxis()
for bar,v in zip(bars,rec_vals):
    ax.text(v + 4, bar.get_y()+bar.get_height()/2, f"{v:.0f} s", va="center",
            ha="left", fontsize=ANNOT_FS, fontweight="bold", color="#111111")
plt.tight_layout(); save("fig4_recovery_time")

# ===========================================================================
# FIG 5 - Node distribution
# ===========================================================================
fig,ax=plt.subplots(figsize=(13,7))
r3_avg=[]; vm1_avg=[]
for p in POLICIES:
    r3=[all_stats[r][p]["node_distribution"].get("r3-node",0) for r in RUN_LABELS if p in all_stats[r]]
    vm1=[all_stats[r][p]["node_distribution"].get("vm1node",0) for r in RUN_LABELS if p in all_stats[r]]
    r3_avg.append(sum(r3)/len(r3) if r3 else 0)
    vm1_avg.append(sum(vm1)/len(vm1) if vm1 else 0)
x_=np.arange(4)
ax.bar(x_,r3_avg,color="#EF9A9A",label="r3-node (Raspberry Pi 3B+)",edgecolor="white")
ax.bar(x_,vm1_avg,bottom=r3_avg,color="#90CAF9",label="vm1node (Cloud VM)",edgecolor="white")
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("% of experiment time"); ax.set_ylim(0,115)
ax.set_title("Pod Node Distribution per Policy",fontweight="bold")
ax.legend(loc="upper right",fontsize=LEGEND_FS); ax.grid(True,axis="y",alpha=0.2)
for i,(r3,vm1) in enumerate(zip(r3_avg,vm1_avg)):
    if r3>5: ax.text(i,r3/2,f"{r3:.0f}%",ha="center",va="center",fontsize=ANNOT_FS,fontweight="bold",color="white")
    if vm1>5: ax.text(i,r3+vm1/2,f"{vm1:.0f}%",ha="center",va="center",fontsize=ANNOT_FS,fontweight="bold",color="white")
plt.tight_layout(); save("fig5_node_distribution")

# ===========================================================================
# FIG 6a - Cost breakdown: all 4 policies (overview)
# FIG 6b - Cost breakdown: active policies detail
# ===========================================================================
compute=[0.005,0.030,0.028,0.041]; sla=[0.624,0.040,0.065,0.010]; migr=[0.000,0.012,0.011,0.021]
totals=[c+s+m for c,s,m in zip(compute,sla,migr)]
C_CLR="#1976D2"; S_CLR="#E53935"; M_CLR="#FB8C00"
bar_w=0.55

# ── FIG 6a: all 4 policies ────────────────────────────────────────────────────
fig,ax=plt.subplots(figsize=(16,9))
x4=np.arange(4)
ax.bar(x4,compute,bar_w,color=C_CLR,label="Compute ($/hr)",alpha=0.9)
ax.bar(x4,sla,bar_w,bottom=compute,color=S_CLR,label="SLA penalty ($/hr)",alpha=0.9)
ax.bar(x4,migr,bar_w,bottom=[c+s for c,s in zip(compute,sla)],
       color=M_CLR,label="Migration overhead ($/hr)",alpha=0.9)
ax.set_xticks(x4); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Cost (USD / hour)")
ax.set_title("Estimated Hourly Cost Breakdown per Policy",fontweight="bold")
ax.set_ylim(0,0.82); ax.grid(True,axis="y",alpha=0.2)
ax.legend(loc="upper right",fontsize=LEGEND_FS)
for i,(c,s,m) in enumerate(zip(compute,sla,migr)):
    if s>=0.08:
        ax.text(i,c+s/2,f"${s:.3f}",ha="center",va="center",fontsize=ANNOT_FS,fontweight="bold",color="white")
for i,t in enumerate(totals):
    ax.text(i,t+0.020,f"${t:.3f}",ha="center",va="bottom",fontsize=ANNOT_FS,fontweight="bold",color="#111111")
plt.tight_layout(); save("fig6a_cost_all_policies")

# ── FIG 6b: active policies detail ──────────────────────────────────────────
fig,ax=plt.subplots(figsize=(14,9))
act_idx=[1,2,3]
act_labels=[LABELS[POLICIES[j]] for j in act_idx]
x3=np.arange(3)
c3=[compute[j] for j in act_idx]; s3=[sla[j] for j in act_idx]; m3=[migr[j] for j in act_idx]
ax.bar(x3,c3,bar_w,color=C_CLR,label="Compute ($/hr)",alpha=0.9)
ax.bar(x3,s3,bar_w,bottom=c3,color=S_CLR,label="SLA penalty ($/hr)",alpha=0.9)
ax.bar(x3,m3,bar_w,bottom=[c+s for c,s in zip(c3,s3)],
       color=M_CLR,label="Migration overhead ($/hr)",alpha=0.9)
ax.set_xticks(x3); ax.set_xticklabels(act_labels)
ax.set_ylabel("Cost (USD / hour)")
ax.set_title("Cost Breakdown — Active Policies Detail",fontweight="bold")
ax.set_ylim(0,0.135); ax.grid(True,axis="y",alpha=0.2)
ax.legend(loc="upper right",fontsize=LEGEND_FS)
for i,(c,s,m) in enumerate(zip(c3,s3,m3)):
    ax.text(i,c/2,     f"${c:.3f}",ha="center",va="center",fontsize=ANNOT_FS,fontweight="bold",color="white")
    ax.text(i,c+s/2,   f"${s:.3f}",ha="center",va="center",fontsize=ANNOT_FS,fontweight="bold",color="white")
    if m>0.005:
        ax.text(i,c+s+m/2,f"${m:.3f}",ha="center",va="center",fontsize=ANNOT_FS,fontweight="bold",color="white")
t3=[c+s+m for c,s,m in zip(c3,s3,m3)]
for i,t in enumerate(t3):
    ax.text(i,t+0.004,f"${t:.3f}",ha="center",va="bottom",fontsize=ANNOT_FS,fontweight="bold",color="#111111")
plt.tight_layout(); save("fig6b_cost_active_detail")


# ===========================================================================
# FIG 7 - Migration count — representative run
# ===========================================================================
fig,ax=plt.subplots(figsize=(13,9))
x_=np.arange(4)
_mig_rep=[get_stat(REP_RUN,p,"migration_count") for p in POLICIES]
bars=ax.bar(x_,_mig_rep,0.5,color=[COLORS[p] for p in POLICIES],alpha=0.88,zorder=3)
for bar,v in zip(bars,_mig_rep):
    if v>0: bar_label_top(ax,bar,str(int(v)),27)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Number of migrations")
ax.set_title("Migration Count per Policy",fontweight="bold")
ax.set_ylim(0,27); ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
ax.grid(True,axis="y",alpha=0.3,zorder=0); plt.tight_layout(); save("fig7_migrations")

# ===========================================================================
# FIG 8 - Break-even
# ===========================================================================
fig,ax=plt.subplots(figsize=(14,7))
vm1_prices=np.linspace(0.01,0.15,300)
ml_cost=0.032+0.808*vm1_prices; threshold_cost=0.054+0.553*vm1_prices
weights_cost=0.079+0.510*vm1_prices; passive_cost=np.full_like(vm1_prices,0.629)
ax.plot(vm1_prices,ml_cost,color=COLORS["ml"],linewidth=3.0,label="ML")
ax.plot(vm1_prices,threshold_cost,color=COLORS["threshold"],linewidth=3.0,label="Threshold")
ax.plot(vm1_prices,weights_cost,color=COLORS["weights"],linewidth=3.0,label="Weights")
ax.plot(vm1_prices,passive_cost,color=COLORS["passive"],linewidth=2.5,linestyle="--",label="Passive")
breakeven_x=0.087; breakeven_y=0.032+0.808*breakeven_x
ax.axvline(breakeven_x,color="#555555",linestyle=":",linewidth=1.8,alpha=0.7)
ax.axvline(0.05,color="#333333",linestyle="--",linewidth=1.5,alpha=0.6,label="Reference ($0.05/hr)")
ax.scatter([breakeven_x],[breakeven_y],color="#555555",s=100,zorder=5)
ax.text(0.090,breakeven_y+0.004,"Break-even $0.087/hr",fontsize=ANNOT_FS,color="#333333",fontweight="bold")
ax.set_xlabel("Cloud VM Price (USD / hour)"); ax.set_ylabel("Total Cost (USD / hour)")
ax.set_title("Break-Even Analysis: ML vs Threshold Policy",fontweight="bold")
ax.legend(loc="upper left",fontsize=LEGEND_FS); ax.grid(True,alpha=0.3); ax.set_xlim(0.01,0.15); ax.set_ylim(0,0.75)
plt.tight_layout(); save("fig8_breakeven")

# ===========================================================================
# FIG 9-11 - QoS time series 2x2 per run
# ===========================================================================
for run_idx,(run_name,df_run) in enumerate(dfs.items(),1):
    fig,axes=plt.subplots(2,2,figsize=(18,11),sharex=True,sharey=True)
    fig.suptitle(f"QoS Over Time - {run_name}",fontsize=TITLE_FS+2,fontweight="bold",y=1.01)
    for ax,policy in zip(axes.flatten(),POLICIES):
        sub=df_run[df_run["policy"]==policy].copy()
        ax.fill_between(sub["elapsed_s"]/60,sub["qos"],alpha=0.15,color=COLORS[policy])
        ax.plot(sub["elapsed_s"]/60,sub["qos"],color=COLORS[policy],linewidth=2.0,label=LABELS[policy])
        mig=sub[sub["migration_event"]==1]
        if not mig.empty:
            ax.scatter(mig["elapsed_s"]/60,mig["qos"],marker="^",color="#555555",s=70,zorder=5,label="Migration",alpha=0.85)
        ax.set_ylim(0,108); ax.set_yticks([0,25,50,70,100])
        ax.legend(loc="lower right",ncol=2,fontsize=LEGEND_FS-1)
        ax.set_title(LABELS[policy],fontsize=TITLE_FS,loc="left",pad=4); ax.grid(True,alpha=0.3)
    for ax in axes[1]: ax.set_xlabel("Time (minutes)")
    for ax in axes[:,0]: ax.set_ylabel("QoS (0-100)")
    plt.tight_layout(); save(f"fig{8+run_idx}_ts_{run_name.lower().replace(' ','')}")

# zigzag multipliers: Run1 low, Run2 high, Run3 low  (pyramid pattern)
_ZZ_MULT = [1.10, 1.55, 1.10]   # used by log-scale figures
_ZZ_LIN  = [0.0,  0.14, 0.0]    # fraction of ylim added; used by linear figures

def bar_label_smart(ax, bar, text, ylim_top, run_idx=0):
    """Inside white for tall bars; zigzag black outside for short bars (linear scale)."""
    h = bar.get_height()
    if h == 0: return
    if h / ylim_top >= 0.32:
        ax.text(bar.get_x()+bar.get_width()/2, h - ylim_top*0.04,
                text, ha="center", va="top", fontsize=ANNOT_FS,
                fontweight="bold", color="white", zorder=5)
    else:
        offset = ylim_top * (0.016 + _ZZ_LIN[run_idx % 3])
        ax.text(bar.get_x()+bar.get_width()/2, h + offset,
                text, ha="center", va="bottom", fontsize=ANNOT_FS,
                fontweight="bold", color="#111111", zorder=5)

def bar_label_log(ax, bar, text, run_idx=0):
    """For log-scale axes: labels above bar with zigzag multiplicative offset."""
    h = bar.get_height()
    if h == 0: return
    y = h * _ZZ_MULT[run_idx % 3]
    ax.text(bar.get_x()+bar.get_width()/2, y,
            text, ha="center", va="bottom", fontsize=ANNOT_FS,
            fontweight="bold", color="#111111", zorder=5)


# ===========================================================================
# FIG 12 - Mean QoS — representative run
# ===========================================================================
x_=np.arange(len(POLICIES))
fig,ax=plt.subplots(figsize=(13,9))
_qos_rep=[get_stat(REP_RUN,p,"mean_qos") for p in POLICIES]
bars=ax.bar(x_,_qos_rep,0.5,color=[COLORS[p] for p in POLICIES],alpha=0.88,zorder=3)
for bar,v in zip(bars,_qos_rep): bar_label_top(ax,bar,f"{v:.1f}",115)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Mean QoS (0-100)")
ax.set_title("Mean QoS per Policy",fontweight="bold")
ax.set_ylim(0,115); ax.grid(True,axis="y",alpha=0.3,zorder=0)
plt.tight_layout(); save("fig12_mean_qos")

# zigzag multipliers: Run1 low, Run2 high, Run3 low  (pyramid pattern)
_ZZ_MULT = [1.10, 1.55, 1.10]   # used by log-scale figures
_ZZ_LIN  = [0.0,  0.14, 0.0]    # fraction of ylim added; used by linear figures

def bar_label_smart(ax, bar, text, ylim_top, run_idx=0):
    """Inside white for tall bars; zigzag black outside for short bars (linear scale)."""
    h = bar.get_height()
    if h == 0: return
    if h / ylim_top >= 0.32:
        ax.text(bar.get_x()+bar.get_width()/2, h - ylim_top*0.04,
                text, ha="center", va="top", fontsize=ANNOT_FS,
                fontweight="bold", color="white", zorder=5)
    else:
        offset = ylim_top * (0.016 + _ZZ_LIN[run_idx % 3])
        ax.text(bar.get_x()+bar.get_width()/2, h + offset,
                text, ha="center", va="bottom", fontsize=ANNOT_FS,
                fontweight="bold", color="#111111", zorder=5)

def bar_label_log(ax, bar, text, run_idx=0):
    """For log-scale axes: labels above bar with zigzag multiplicative offset."""
    h = bar.get_height()
    if h == 0: return
    y = h * _ZZ_MULT[run_idx % 3]
    ax.text(bar.get_x()+bar.get_width()/2, y,
            text, ha="center", va="bottom", fontsize=ANNOT_FS,
            fontweight="bold", color="#111111", zorder=5)


# ===========================================================================
# FIG 13 - % below SLA — representative run (Run 2, closest to mean)
# ===========================================================================
fig,ax=plt.subplots(figsize=(13,9))
rep_pct=[get_stat(REP_RUN,p,"pct_below_sla") for p in POLICIES]
bars=ax.bar(x_,[rep_pct[i] for i in range(4)],0.5,
            color=[COLORS[p] for p in POLICIES],alpha=0.88,zorder=3)
for bar,v in zip(bars,rep_pct): bar_label_top(ax,bar,f"{v:.1f}%",68)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("% time below QoS 70")
ax.set_title("% Time Below SLA per Policy",fontweight="bold")
ax.set_ylim(0,72); ax.grid(True,axis="y",alpha=0.3,zorder=0)
plt.tight_layout(); save("fig13_pct_below_sla")

# zigzag multipliers: Run1 low, Run2 high, Run3 low  (pyramid pattern)
_ZZ_MULT = [1.10, 1.55, 1.10]   # used by log-scale figures
_ZZ_LIN  = [0.0,  0.14, 0.0]    # fraction of ylim added; used by linear figures

def bar_label_smart(ax, bar, text, ylim_top, run_idx=0):
    """Inside white for tall bars; zigzag black outside for short bars (linear scale)."""
    h = bar.get_height()
    if h == 0: return
    if h / ylim_top >= 0.32:
        ax.text(bar.get_x()+bar.get_width()/2, h - ylim_top*0.04,
                text, ha="center", va="top", fontsize=ANNOT_FS,
                fontweight="bold", color="white", zorder=5)
    else:
        offset = ylim_top * (0.016 + _ZZ_LIN[run_idx % 3])
        ax.text(bar.get_x()+bar.get_width()/2, h + offset,
                text, ha="center", va="bottom", fontsize=ANNOT_FS,
                fontweight="bold", color="#111111", zorder=5)

def bar_label_log(ax, bar, text, run_idx=0):
    """For log-scale axes: labels above bar with zigzag multiplicative offset."""
    h = bar.get_height()
    if h == 0: return
    y = h * _ZZ_MULT[run_idx % 3]
    ax.text(bar.get_x()+bar.get_width()/2, y,
            text, ha="center", va="bottom", fontsize=ANNOT_FS,
            fontweight="bold", color="#111111", zorder=5)


# ===========================================================================
# FIG 14 - SLA violations — representative run
# ===========================================================================
fig,ax=plt.subplots(figsize=(13,9))
_viol_rep=[get_stat(REP_RUN,p,"n_violations") for p in POLICIES]
bars=ax.bar(x_,_viol_rep,0.5,color=[COLORS[p] for p in POLICIES],alpha=0.88,zorder=3)
for bar,v in zip(bars,_viol_rep):
    if v>0: bar_label_top(ax,bar,str(int(v)),13)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("SLA violation events")
ax.set_title("SLA Violation Events per Policy",fontweight="bold")
ax.set_ylim(0,13); ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
ax.grid(True,axis="y",alpha=0.3,zorder=0); plt.tight_layout(); save("fig14_violations")

# zigzag multipliers: Run1 low, Run2 high, Run3 low  (pyramid pattern)
_ZZ_MULT = [1.10, 1.55, 1.10]   # used by log-scale figures
_ZZ_LIN  = [0.0,  0.14, 0.0]    # fraction of ylim added; used by linear figures

def bar_label_smart(ax, bar, text, ylim_top, run_idx=0):
    """Inside white for tall bars; zigzag black outside for short bars (linear scale)."""
    h = bar.get_height()
    if h == 0: return
    if h / ylim_top >= 0.32:
        ax.text(bar.get_x()+bar.get_width()/2, h - ylim_top*0.04,
                text, ha="center", va="top", fontsize=ANNOT_FS,
                fontweight="bold", color="white", zorder=5)
    else:
        offset = ylim_top * (0.016 + _ZZ_LIN[run_idx % 3])
        ax.text(bar.get_x()+bar.get_width()/2, h + offset,
                text, ha="center", va="bottom", fontsize=ANNOT_FS,
                fontweight="bold", color="#111111", zorder=5)

def bar_label_log(ax, bar, text, run_idx=0):
    """For log-scale axes: labels above bar with zigzag multiplicative offset."""
    h = bar.get_height()
    if h == 0: return
    y = h * _ZZ_MULT[run_idx % 3]
    ax.text(bar.get_x()+bar.get_width()/2, y,
            text, ha="center", va="bottom", fontsize=ANNOT_FS,
            fontweight="bold", color="#111111", zorder=5)


# ===========================================================================
# FIG 15 - Recovery time — representative run
# ===========================================================================
fig,ax=plt.subplots(figsize=(13,9))
_rec_rep=[get_stat(REP_RUN,p,"avg_recovery_s") for p in POLICIES]
bars=ax.bar(x_,_rec_rep,0.5,color=[COLORS[p] for p in POLICIES],alpha=0.88,zorder=3)
for bar,v in zip(bars,_rec_rep): bar_label_top(ax,bar,f"{v:.0f}s",405)
ax.axhline(300,color="#777",linestyle="--",linewidth=1.5,alpha=0.5,label="Burn phase (300 s)")
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Recovery time (seconds)")
ax.set_title("QoS Recovery Time per Policy",fontweight="bold")
ax.legend(loc="upper right",fontsize=LEGEND_FS); ax.set_ylim(0,405); ax.grid(True,axis="y",alpha=0.3,zorder=0)
plt.tight_layout(); save("fig15_recovery")

# zigzag multipliers: Run1 low, Run2 high, Run3 low  (pyramid pattern)
_ZZ_MULT = [1.10, 1.55, 1.10]   # used by log-scale figures
_ZZ_LIN  = [0.0,  0.14, 0.0]    # fraction of ylim added; used by linear figures

def bar_label_smart(ax, bar, text, ylim_top, run_idx=0):
    """Inside white for tall bars; zigzag black outside for short bars (linear scale)."""
    h = bar.get_height()
    if h == 0: return
    if h / ylim_top >= 0.32:
        ax.text(bar.get_x()+bar.get_width()/2, h - ylim_top*0.04,
                text, ha="center", va="top", fontsize=ANNOT_FS,
                fontweight="bold", color="white", zorder=5)
    else:
        offset = ylim_top * (0.016 + _ZZ_LIN[run_idx % 3])
        ax.text(bar.get_x()+bar.get_width()/2, h + offset,
                text, ha="center", va="bottom", fontsize=ANNOT_FS,
                fontweight="bold", color="#111111", zorder=5)

def bar_label_log(ax, bar, text, run_idx=0):
    """For log-scale axes: labels above bar with zigzag multiplicative offset."""
    h = bar.get_height()
    if h == 0: return
    y = h * _ZZ_MULT[run_idx % 3]
    ax.text(bar.get_x()+bar.get_width()/2, y,
            text, ha="center", va="bottom", fontsize=ANNOT_FS,
            fontweight="bold", color="#111111", zorder=5)


# ===========================================================================
# FIG 16 - QoS burn — representative run
# ===========================================================================
fig,ax=plt.subplots(figsize=(13,9))
_burn_rep=[get_stat(REP_RUN,p,"avg_qos_burn") for p in POLICIES]
bars=ax.bar(x_,_burn_rep,0.5,color=[COLORS[p] for p in POLICIES],alpha=0.88,zorder=3)
for bar,v in zip(bars,_burn_rep): bar_label_top(ax,bar,f"{v:.1f}",115)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Avg QoS during CPU burn (0-100)")
ax.set_title("QoS During CPU Burn Phase",fontweight="bold")
ax.set_ylim(0,115); ax.grid(True,axis="y",alpha=0.3,zorder=0)
plt.tight_layout(); save("fig16_qos_burn")

# zigzag multipliers: Run1 low, Run2 high, Run3 low  (pyramid pattern)
_ZZ_MULT = [1.10, 1.55, 1.10]   # used by log-scale figures
_ZZ_LIN  = [0.0,  0.14, 0.0]    # fraction of ylim added; used by linear figures

def bar_label_smart(ax, bar, text, ylim_top, run_idx=0):
    """Inside white for tall bars; zigzag black outside for short bars (linear scale)."""
    h = bar.get_height()
    if h == 0: return
    if h / ylim_top >= 0.32:
        ax.text(bar.get_x()+bar.get_width()/2, h - ylim_top*0.04,
                text, ha="center", va="top", fontsize=ANNOT_FS,
                fontweight="bold", color="white", zorder=5)
    else:
        offset = ylim_top * (0.016 + _ZZ_LIN[run_idx % 3])
        ax.text(bar.get_x()+bar.get_width()/2, h + offset,
                text, ha="center", va="bottom", fontsize=ANNOT_FS,
                fontweight="bold", color="#111111", zorder=5)

def bar_label_log(ax, bar, text, run_idx=0):
    """For log-scale axes: labels above bar with zigzag multiplicative offset."""
    h = bar.get_height()
    if h == 0: return
    y = h * _ZZ_MULT[run_idx % 3]
    ax.text(bar.get_x()+bar.get_width()/2, y,
            text, ha="center", va="bottom", fontsize=ANNOT_FS,
            fontweight="bold", color="#111111", zorder=5)


# ===========================================================================
# FIG 17 - Time below SLA in seconds (m'ss" format)
# ===========================================================================
def fmt_time(v):
    mins,s=divmod(int(round(v)),60)
    return f"{mins}'{s:02d}\"" if mins else f"{s}\""

fig,ax=plt.subplots(figsize=(13,9))
rep_time=[round(get_stat(REP_RUN,p,"pct_below_sla")/100*TOTAL_S) for p in POLICIES]
bars=ax.bar(x_,rep_time,0.5,color=[COLORS[p] for p in POLICIES],alpha=0.88,zorder=3)
for bar,v in zip(bars,rep_time): bar_label_top(ax,bar,fmt_time(v),2500)
ax.axhline(TOTAL_S,color="#777",linestyle="--",linewidth=1.5,alpha=0.6,label=f"Full experiment (59'55\")")
ax.axhline(300,color="#aaa",linestyle=":",linewidth=1.3,alpha=0.6,label="CPU burn window (5'00\")")
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Time below QoS 70")
ax.set_title("Time Spent Below SLA Threshold",fontweight="bold")
ax.set_yscale("log")
_ticks17=[10,30,100,300,600,1200,2400]
ax.set_yticks(_ticks17); ax.set_yticklabels([fmt_time(v) for v in _ticks17],fontsize=TICK_FS)
ax.set_ylim(6,3200)
ax.legend(loc="upper right",fontsize=LEGEND_FS); ax.grid(True,axis="y",alpha=0.3,zorder=0,which="both")
plt.tight_layout(); save("fig17_sla_time_seconds")


# ===========================================================================

# ===========================================================================
# FIG 8b — Break-even: one subplot per policy vs passive reference
# ===========================================================================
vm1_p=np.linspace(0.01,0.15,300)
_pcosts={"passive":np.full_like(vm1_p,0.629),
         "threshold":0.054+0.553*vm1_p,
         "weights":0.079+0.510*vm1_p,
         "ml":0.032+0.808*vm1_p}
fig,axes=plt.subplots(1,4,figsize=(24,8),sharey=True)
fig.suptitle("Cost vs Cloud VM Price — Per Policy",fontsize=TITLE_FS,fontweight="bold",y=1.02)
for ax,pol in zip(axes,POLICIES):
    ax.plot(vm1_p,_pcosts[pol],color=COLORS[pol],linewidth=3.0,label=LABELS[pol])
    if pol!="passive":
        ax.plot(vm1_p,_pcosts["passive"],color=COLORS["passive"],linewidth=2.0,
                linestyle="--",alpha=0.55,label="Passive ref")
    ax.axvline(0.05,color="#444",linestyle=":",linewidth=1.5,alpha=0.6)
    ax.set_xlabel("VM Price ($/hr)"); ax.set_title(LABELS[pol],fontweight="bold")
    ax.grid(True,alpha=0.3); ax.set_xlim(0.01,0.15); ax.set_ylim(0,0.75)
    ax.legend(loc="upper left",fontsize=LEGEND_FS-2)
axes[0].set_ylabel("Total Cost ($/hr)")
plt.tight_layout(); save("fig8b_breakeven_per_policy")

# ===========================================================================
# FIG 9b / 10b — Individual policy time series: Run 1 and Run 3
# (Run 2 already covered by fig1_passive / threshold / weights / ml)
# ===========================================================================
for run_name,df_run,prefix in [("Run 1",dfs["Run 1"],"fig9b"),
                                ("Run 3",dfs["Run 3"],"fig10b")]:
    for policy in POLICIES:
        fig,ax=plt.subplots(figsize=(24,11))
        sub=df_run[df_run["policy"]==policy].copy()
        ax.fill_between(sub["elapsed_s"]/60,sub["qos"],alpha=0.15,color=COLORS[policy])
        ax.plot(sub["elapsed_s"]/60,sub["qos"],color=COLORS[policy],linewidth=3.0,label=LABELS[policy])
        mig=sub[sub["migration_event"]==1]
        if not mig.empty:
            ax.scatter(mig["elapsed_s"]/60,mig["qos"],marker="^",color="#555555",
                       s=100,zorder=5,label="Migration",alpha=0.85)
        ax.axhline(70,color="#555555",linestyle="--",linewidth=2.0,alpha=0.75,
                   label="SLA threshold (QoS 70)")
        ax.set_ylim(0,108); ax.set_yticks([0,25,50,75,100])
        ax.set_xlabel("Time (minutes)",fontsize=_F1_LABEL,fontweight="bold")
        ax.set_ylabel("QoS",fontsize=_F1_LABEL,fontweight="bold")
        ax.set_title(f"QoS Over Time — {LABELS[policy]} ({run_name})",
                     fontweight="bold",fontsize=_F1_TITLE)
        ax.tick_params(axis="both",labelsize=_F1_TICK,width=2,length=6)
        ax.legend(loc="lower right",fontsize=_F1_LEG,framealpha=0.9)
        ax.grid(True,alpha=0.3)
        plt.tight_layout(); save(f"{prefix}_{policy}")


# ===========================================================================
# ALL-RUNS VARIANTS — 3 grouped bars per policy (for comparison / decisions)
# ===========================================================================
x_=np.arange(len(POLICIES)); w=0.22

# ── fig7_allruns ─────────────────────────────────────────────────────────────
fig,ax=plt.subplots(figsize=(15,9))
mig_r=[[get_stat(r,p,"migration_count") for p in POLICIES] for r in RUN_LABELS]
for i,(rn,rc,vals) in enumerate(zip(RUN_LABELS,RUN_COLORS,mig_r)):
    bars=ax.bar(x_+(i-1)*w,vals,w,label=rn,color=rc,alpha=0.9,zorder=3)
    for bar,v in zip(bars,vals):
        if v>0: bar_label_top(ax,bar,str(int(v)),27)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Number of migrations"); ax.set_title("Migration Count per Policy — All Runs",fontweight="bold")
ax.legend(loc="upper left",fontsize=LEGEND_FS); ax.set_ylim(0,27)
ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
ax.grid(True,axis="y",alpha=0.3,zorder=0); plt.tight_layout(); save("fig7_migrations_allruns")

# ── fig12_allruns ────────────────────────────────────────────────────────────
fig,ax=plt.subplots(figsize=(15,9))
vals_r=[[get_stat(r,p,"mean_qos") for p in POLICIES] for r in RUN_LABELS]
_YL12=122; _ZZ12=[0.0,0.055,0.0]
for i,(rn,rc,vals) in enumerate(zip(RUN_LABELS,RUN_COLORS,vals_r)):
    bars=ax.bar(x_+(i-1)*w,vals,w,label=rn,color=rc,alpha=0.88,zorder=3)
    for bar,v in zip(bars,vals):
        offset=_YL12*(0.012+_ZZ12[i%3])
        ax.text(bar.get_x()+bar.get_width()/2,v+offset,f"{v:.1f}",
                ha="center",va="bottom",fontsize=ANNOT_FS,fontweight="bold",color="#111111",zorder=5)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Mean QoS (0-100)"); ax.set_title("Mean QoS per Policy — All Runs",fontweight="bold")
ax.legend(loc="upper left",fontsize=LEGEND_FS); ax.set_ylim(0,_YL12)
ax.grid(True,axis="y",alpha=0.3,zorder=0); plt.tight_layout(); save("fig12_mean_qos_allruns")

# ── fig13_allruns ────────────────────────────────────────────────────────────
fig,ax=plt.subplots(figsize=(15,9))
vals_r=[[get_stat(r,p,"pct_below_sla") for p in POLICIES] for r in RUN_LABELS]
for i,(rn,rc,vals) in enumerate(zip(RUN_LABELS,RUN_COLORS,vals_r)):
    bars=ax.bar(x_+(i-1)*w,vals,w,label=rn,color=rc,alpha=0.88,zorder=3)
    for bar,v in zip(bars,vals): bar_label_log(ax,bar,f"{v:.1f}%",run_idx=i)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("% time below QoS 70"); ax.set_title("% Time Below SLA — All Runs",fontweight="bold")
ax.set_yscale("log")
_t13=[0.5,1,2,5,10,20,50]
ax.set_yticks(_t13); ax.set_yticklabels([f"{v}%" for v in _t13],fontsize=TICK_FS)
ax.set_ylim(0.35,130); ax.legend(loc="upper right",fontsize=LEGEND_FS)
ax.grid(True,axis="y",alpha=0.3,zorder=0,which="both"); plt.tight_layout(); save("fig13_pct_below_sla_allruns")

# ── fig14_allruns ────────────────────────────────────────────────────────────
fig,ax=plt.subplots(figsize=(15,9))
vals_r=[[get_stat(r,p,"n_violations") for p in POLICIES] for r in RUN_LABELS]
for i,(rn,rc,vals) in enumerate(zip(RUN_LABELS,RUN_COLORS,vals_r)):
    bars=ax.bar(x_+(i-1)*w,vals,w,label=rn,color=rc,alpha=0.88,zorder=3)
    for bar,v in zip(bars,vals):
        if v>0: bar_label_top(ax,bar,str(int(v)),13)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("SLA violation events"); ax.set_title("SLA Violation Events — All Runs",fontweight="bold")
ax.legend(loc="upper right",fontsize=LEGEND_FS); ax.set_ylim(0,13)
ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
ax.grid(True,axis="y",alpha=0.3,zorder=0); plt.tight_layout(); save("fig14_violations_allruns")

# ── fig15_allruns ────────────────────────────────────────────────────────────
fig,ax=plt.subplots(figsize=(15,9))
vals_r=[[get_stat(r,p,"avg_recovery_s") for p in POLICIES] for r in RUN_LABELS]
for i,(rn,rc,vals) in enumerate(zip(RUN_LABELS,RUN_COLORS,vals_r)):
    bars=ax.bar(x_+(i-1)*w,vals,w,label=rn,color=rc,alpha=0.88,zorder=3)
    for bar,v in zip(bars,vals): bar_label_top(ax,bar,f"{v:.0f}s",405)
ax.axhline(300,color="#777",linestyle="--",linewidth=1.5,alpha=0.5,label="Burn phase (300 s)")
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Recovery time (seconds)"); ax.set_title("QoS Recovery Time — All Runs",fontweight="bold")
ax.legend(loc="upper right",fontsize=LEGEND_FS); ax.set_ylim(0,405)
ax.grid(True,axis="y",alpha=0.3,zorder=0); plt.tight_layout(); save("fig15_recovery_allruns")

# ── fig16_allruns ────────────────────────────────────────────────────────────
fig,ax=plt.subplots(figsize=(15,9))
vals_r=[[get_stat(r,p,"avg_qos_burn") for p in POLICIES] for r in RUN_LABELS]
for i,(rn,rc,vals) in enumerate(zip(RUN_LABELS,RUN_COLORS,vals_r)):
    bars=ax.bar(x_+(i-1)*w,vals,w,label=rn,color=rc,alpha=0.88,zorder=3)
    for bar,v in zip(bars,vals): bar_label_top(ax,bar,f"{v:.1f}",115)
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Avg QoS during CPU burn (0-100)"); ax.set_title("QoS During CPU Burn Phase — All Runs",fontweight="bold")
ax.legend(loc="upper left",fontsize=LEGEND_FS); ax.set_ylim(0,115)
ax.grid(True,axis="y",alpha=0.3,zorder=0); plt.tight_layout(); save("fig16_qos_burn_allruns")

# ── fig17_allruns ────────────────────────────────────────────────────────────
fig,ax=plt.subplots(figsize=(15,10))
vals_r=[[round(get_stat(r,p,"pct_below_sla")/100*TOTAL_S) for p in POLICIES] for r in RUN_LABELS]
for i,(rn,rc,vals) in enumerate(zip(RUN_LABELS,RUN_COLORS,vals_r)):
    bars=ax.bar(x_+(i-1)*w,vals,w,label=rn,color=rc,alpha=0.88,zorder=3)
    for bar,v in zip(bars,vals): bar_label_log(ax,bar,fmt_time(v),run_idx=i)
ax.axhline(TOTAL_S,color="#777",linestyle="--",linewidth=1.5,alpha=0.6,label=f"Full experiment (59'55\")")
ax.axhline(300,color="#aaa",linestyle=":",linewidth=1.3,alpha=0.6,label="CPU burn window (5'00\")")
ax.set_xticks(x_); ax.set_xticklabels([LABELS[p] for p in POLICIES])
ax.set_ylabel("Time below QoS 70"); ax.set_title("Time Spent Below SLA Threshold — All Runs",fontweight="bold")
ax.set_yscale("log")
_t17=[10,30,100,300,600,1200,2400]
ax.set_yticks(_t17); ax.set_yticklabels([fmt_time(v) for v in _t17],fontsize=TICK_FS)
ax.set_ylim(6,4200); ax.legend(loc="upper right",fontsize=LEGEND_FS)
ax.grid(True,axis="y",alpha=0.3,zorder=0,which="both"); plt.tight_layout(); save("fig17_sla_time_allruns")

print("ALL FIGURES DONE")
