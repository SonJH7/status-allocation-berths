import React, { useEffect, useMemo, useRef, useState } from "react";
import { Streamlit, type Theme } from "streamlit-component-lib";

type RawItem = {
  row_id: number;
  vessel?: string;
  voyage?: string;
  terminal?: string;
  berth?: number;
  start?: string;
  end?: string;
  f?: number;
  e?: number;
  note?: string;
  plan_status?: string;
  pilot?: string;
};

type MoveEvent = { row_id: number; dmin: number; dy: number };
type EventPayload = { event_id: string; events: MoveEvent[] };

type EnrichedItem = RawItem & {
  startDate: Date | null;
  endDate: Date | null;
  f: number;
  e: number;
};

type TerminalKey = "SND" | "GAM";

type TimelineProps = {
  items: RawItem[];
  disabled?: boolean;
  theme?: Theme;
  frameWidth?: number;
  onEvents?: (payload: EventPayload) => void;
};

type DragState = {
  rowId: number;
  pointerId: number;
  terminal: TerminalKey;
  startClientX: number;
  startClientY: number;
  baseStart: number;
  baseEnd: number;
  baseF: number;
  baseE: number;
};

type DraftPatch = { rowId: number; patch: Partial<EnrichedItem> | null };

const TERMINAL_META: Record<
  TerminalKey,
  { label: string; yMax: number; berthStep: number; berths: number[] }
> = {
  SND: { label: "신항 SND", yMax: 1500, berthStep: 300, berths: [1, 2, 3, 4, 5] },
  GAM: { label: "감만 GAM", yMax: 1400, berthStep: 350, berths: [6, 7, 8, 9] },
};

const STATUS_COLORS: Record<string, string> = {
  LOAD_PLANNING_DONE: "rgba(236,130,176,0.82)",
  DISCHARGE_PLANNING_DONE: "rgba(115,158,245,0.82)",
  CRANE_ASSIGNED: "rgba(248,202,109,0.86)",
  CRANE_UNASSIGNED: "rgba(180,180,186,0.80)",
};

const clamp = (v: number, min: number, max: number) => Math.min(Math.max(v, min), max);
const snapMinutes = (v: number) => Math.round(v / 5) * 5;
const snapMeters = (v: number) => Math.round(v / 30) * 30;

const parseDate = (value: any): Date | null => {
  if (!value) return null;
  const d = new Date(value);
  return Number.isFinite(d.getTime()) ? d : null;
};

const fillForStatus = (status: string | undefined, terminal: TerminalKey) => {
  if (status && STATUS_COLORS[status]) {
    return STATUS_COLORS[status];
  }
  return terminal === "SND" ? "rgba(120,160,240,0.78)" : "rgba(240,180,80,0.78)";
};

const textColorForFill = (fill: string) => {
  try {
    const parts = fill.match(/[\d.]+/g);
    if (!parts || parts.length < 3) return "rgba(20,20,24,0.95)";
    const [r, g, b, aRaw] = parts.map(parseFloat);
    const a = Number.isFinite(aRaw) ? aRaw : 1;
    const mix = (c: number) => 255 * (1 - a) + c * a;
    const r2 = mix(r);
    const g2 = mix(g);
    const b2 = mix(b);
    const lum =
      0.2126 * (r2 / 255) ** 2.2 + 0.7152 * (g2 / 255) ** 2.2 + 0.0722 * (b2 / 255) ** 2.2;
    return lum > 0.62 ? "rgba(22,22,30,0.96)" : "rgba(255,255,255,0.96)";
  } catch (_e) {
    return "rgba(20,20,24,0.95)";
  }
};

const computeRange = (rows: EnrichedItem[]): [number, number] => {
  const now = Date.now();
  let x0 = now - 24 * 60 * 60 * 1000;
  let x1 = now + 6 * 24 * 60 * 60 * 1000;

  const starts = rows
    .map((r) => r.startDate?.getTime() ?? null)
    .filter((t) => typeof t === "number") as number[];
  const ends = rows
    .map((r) => r.endDate?.getTime() ?? null)
    .filter((t) => typeof t === "number") as number[];

  if (starts.length) {
    x0 = Math.min(x0, Math.min(...starts) - 6 * 60 * 60 * 1000);
  }
  if (ends.length) {
    x1 = Math.max(x1, Math.max(...ends) + 12 * 60 * 60 * 1000);
  }
  if (x1 <= x0) {
    x1 = x0 + 24 * 60 * 60 * 1000;
  }
  return [x0, x1];
};

const tickLabels = (startMs: number, endMs: number) => {
  const labels: { x: number; text: string }[] = [];
  const sixHours = 6 * 60 * 60 * 1000;
  const startAligned = Math.floor(startMs / sixHours) * sixHours;
  for (let t = startAligned; t <= endMs + sixHours; t += sixHours) {
    const d = new Date(t);
    const isMidnight = d.getHours() === 0;
    const text = isMidnight ? `${d.getMonth() + 1}/${d.getDate()}` : `${String(d.getHours()).padStart(2, "0")}h`;
    labels.push({ x: t, text });
  }
  return labels;
};

const Timeline: React.FC<TimelineProps> = ({
  items,
  disabled = false,
  theme,
  frameWidth,
  onEvents,
}) => {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const dragRef = useRef<DragState | null>(null);
  const rafRef = useRef<number | null>(null);
  const pendingDraftRef = useRef<DraftPatch | null>(null);
  const eventSeqRef = useRef(0);
  const [draft, setDraft] = useState<Record<number, Partial<EnrichedItem>>>({});
  const [booting, setBooting] = useState(true);

  useEffect(() => {
    setDraft({});
    dragRef.current = null;
  }, [items]);

  useEffect(() => {
    return () => {
      if (rafRef.current !== null) {
        cancelAnimationFrame(rafRef.current);
      }
    };
  }, []);

  useEffect(() => {
    setBooting(true);
    const id = window.setTimeout(() => setBooting(false), 250);
    return () => window.clearTimeout(id);
  }, [items.length]);

  const parsedItems = useMemo<EnrichedItem[]>(() => {
    return (items || []).map((raw) => ({
      ...raw,
      startDate: parseDate(raw.start),
      endDate: parseDate(raw.end),
      f: Number(raw.f ?? 0),
      e: Number(raw.e ?? 0),
    }));
  }, [items]);

  const committedRange = useMemo(() => computeRange(parsedItems), [parsedItems]);
  const liveItems = useMemo<EnrichedItem[]>(() => {
    return parsedItems.map((row) => {
      const patch = draft[row.row_id];
      return patch ? ({ ...row, ...patch } as EnrichedItem) : row;
    });
  }, [parsedItems, draft]);

  const margin = { left: 84, right: 48, top: 18, bottom: 28 };
  const [x0, x1] = committedRange; // 드래그 중에는 draft에 따라 range를 재계산하지 않음
  const hoursRange = Math.max(1, (x1 - x0) / (60 * 60 * 1000));
  const minInnerWidth =
    typeof frameWidth === "number"
      ? Math.max(960, frameWidth - margin.left - margin.right)
      : 1200;
  const innerWidth = Math.max(minInnerWidth, Math.round(hoursRange * 14), 1200);
  const svgWidth = innerWidth + margin.left + margin.right;
  const pxPerMs = innerWidth / (x1 - x0);
  const pxPerMin = pxPerMs * 60 * 1000;
  const nowMs = Date.now();
  const ticks = useMemo(() => tickLabels(x0, x1), [x0, x1]);

  useEffect(() => {
    if (rootRef.current) {
      const h = rootRef.current.getBoundingClientRect().height;
      Streamlit.setFrameHeight(h + 24);
    } else {
      Streamlit.setFrameHeight(960);
    }
  }, [svgWidth, parsedItems.length]);

  const scheduleDraft = (rowId: number, patch: Partial<EnrichedItem> | null) => {
    pendingDraftRef.current = { rowId, patch };
    if (rafRef.current !== null) return;
    rafRef.current = requestAnimationFrame(() => {
      const pending = pendingDraftRef.current;
      pendingDraftRef.current = null;
      rafRef.current = null;
      if (!pending) return;
      setDraft((prev) => {
        const next = { ...prev };
        if (pending.patch === null) {
          delete next[pending.rowId];
        } else {
          next[pending.rowId] = pending.patch;
        }
        return next;
      });
    });
  };

  const renderTerminal = (terminal: TerminalKey) => {
    const meta = TERMINAL_META[terminal];
    const itemsForTerminal = liveItems.filter(
      (r) => (r.terminal || "").toUpperCase() === terminal && r.startDate && r.endDate
    );
    const innerHeight = Math.max(380, Math.round(meta.yMax * 0.32));
    const svgHeight = innerHeight + margin.top + margin.bottom;
    const pxPerMeter = innerHeight / meta.yMax;

    const toX = (t: Date) => margin.left + (t.getTime() - x0) * pxPerMs;
    const toY = (v: number) => margin.top + v * pxPerMeter;

    const onDragMove = (evt: PointerEvent, state: DragState) => {
      const dx = evt.clientX - state.startClientX;
      const dyPx = evt.clientY - state.startClientY;
      const dmin = snapMinutes(dx / pxPerMin);
      const dyMeters = snapMeters(dyPx / pxPerMeter);

      if (dmin === 0 && dyMeters === 0) {
        scheduleDraft(state.rowId, null);
        return;
      }

      const length = Math.abs(state.baseE - state.baseF);
      const mid = (state.baseE + state.baseF) / 2;
      const newMid = clamp(mid + dyMeters, length / 2, meta.yMax - length / 2);
      const newF = newMid - length / 2;
      const newE = newMid + length / 2;

      const newStart = new Date(state.baseStart + dmin * 60 * 1000);
      const newEnd = new Date(state.baseEnd + dmin * 60 * 1000);

      scheduleDraft(state.rowId, { startDate: newStart, endDate: newEnd, f: newF, e: newE });
    };

    const onDragEnd = (evt: PointerEvent, state: DragState) => {
      const dx = evt.clientX - state.startClientX;
      const dyPx = evt.clientY - state.startClientY;
      const dmin = snapMinutes(dx / pxPerMin);
      const dyMeters = snapMeters(dyPx / pxPerMeter);

      scheduleDraft(state.rowId, null);
      dragRef.current = null;

      if (!onEvents || (dmin === 0 && dyMeters === 0)) {
        return;
      }

      eventSeqRef.current += 1;
      const event_id = `${state.rowId}-${Date.now()}-${eventSeqRef.current}`;
      onEvents({ event_id, events: [{ row_id: state.rowId, dmin, dy: dyMeters }] });
    };

    const onDragStart = (item: EnrichedItem) => (evt: React.PointerEvent<SVGRectElement>) => {
      if (disabled || !item.startDate || !item.endDate) return;
      evt.stopPropagation();
      evt.preventDefault();
      try {
        (evt.target as HTMLElement).setPointerCapture(evt.pointerId);
      } catch (_e) {
        /* ignore */
      }

      const state: DragState = {
        rowId: item.row_id,
        pointerId: evt.pointerId,
        terminal,
        startClientX: evt.clientX,
        startClientY: evt.clientY,
        baseStart: item.startDate.getTime(),
        baseEnd: item.endDate.getTime(),
        baseF: item.f,
        baseE: item.e,
      };
      dragRef.current = state;

      const moveListener = (moveEvt: PointerEvent) => {
        if (dragRef.current?.pointerId !== moveEvt.pointerId) return;
        onDragMove(moveEvt, state);
      };
      const upListener = (upEvt: PointerEvent) => {
        if (dragRef.current?.pointerId !== upEvt.pointerId) return;
        onDragEnd(upEvt, state);
        try {
          (evt.target as HTMLElement).releasePointerCapture(state.pointerId);
        } catch (_e) {
          /* no-op */
        }
        window.removeEventListener("pointermove", moveListener);
        window.removeEventListener("pointerup", upListener);
        window.removeEventListener("pointercancel", upListener);
      };
      window.addEventListener("pointermove", moveListener, { passive: true });
      window.addEventListener("pointerup", upListener, { passive: true });
      window.addEventListener("pointercancel", upListener, { passive: true });
    };

    return (
      <div key={terminal} className="terminal-block">
        <div className="terminal-head">
          <div className="terminal-name">{meta.label}</div>
          <div className="terminal-meta">
            <span>Berth step {meta.berthStep}m · Snap: 5min / 30m</span>
            <span className="total-count">{itemsForTerminal.length} vessels</span>
          </div>
        </div>
        <div className="timeline-canvas">
          <svg width={svgWidth} height={svgHeight} role="presentation">
            <defs>
              <linearGradient id={`bg-${terminal}`} x1="0" x2="0" y1="0" y2="1">
                <stop offset="0%" stopColor="rgba(255,255,255,0.96)" />
                <stop offset="100%" stopColor="rgba(247,249,255,0.9)" />
              </linearGradient>
            </defs>
            <rect
              x={0}
              y={0}
              width={svgWidth}
              height={svgHeight}
              fill={`url(#bg-${terminal})`}
              rx={8}
            />

            {ticks.map(({ x, text }, idx) => {
              const px = margin.left + (x - x0) * pxPerMs;
              const isDay = text.includes("/");
              return (
                <g key={`${terminal}-tick-${idx}`}>
                  <line
                    x1={px}
                    x2={px}
                    y1={margin.top}
                    y2={margin.top + innerHeight}
                    stroke={isDay ? "rgba(0,0,0,0.18)" : "rgba(0,0,0,0.08)"}
                    strokeWidth={isDay ? 1.2 : 0.8}
                  />
                  {idx % 2 === 0 && (
                    <text
                      x={px + 2}
                      y={14}
                      fontSize={11}
                      fill="rgba(30,30,30,0.8)"
                      textAnchor="start"
                    >
                      {text}
                    </text>
                  )}
                </g>
              );
            })}

            {x0 <= nowMs && nowMs <= x1 && (
              <g>
                <line
                  x1={margin.left + (nowMs - x0) * pxPerMs}
                  x2={margin.left + (nowMs - x0) * pxPerMs}
                  y1={margin.top}
                  y2={margin.top + innerHeight}
                  stroke="rgba(220,60,60,0.95)"
                  strokeWidth={1.6}
                  strokeDasharray="4 6"
                />
                <text
                  x={margin.left + (nowMs - x0) * pxPerMs + 4}
                  y={margin.top + 14}
                  fontSize={11}
                  fill="rgba(180,40,40,0.95)"
                >
                  now
                </text>
              </g>
            )}

            {meta.berths.map((berth, idx) => {
              const y0Band = toY(idx * meta.berthStep);
              const y1Band = toY((idx + 1) * meta.berthStep);
              const mid = (y0Band + y1Band) / 2;
              return (
                <g key={`${terminal}-berth-${berth}`}>
                  <rect
                    x={margin.left}
                    y={y0Band}
                    width={innerWidth}
                    height={y1Band - y0Band}
                    fill={idx % 2 === 0 ? "rgba(0,0,0,0.028)" : "rgba(0,0,0,0.018)"}
                    stroke="rgba(0,0,0,0.08)"
                    strokeWidth={0.4}
                  />
                  <text
                    x={margin.left - 10}
                    y={mid}
                    textAnchor="end"
                    dominantBaseline="middle"
                    fontSize={12}
                    fill="rgba(35,35,35,0.8)"
                  >
                    {berth}
                  </text>
                </g>
              );
            })}

            {itemsForTerminal.map((r) => {
              const xStart = toX(r.startDate as Date);
              const xEnd = toX(r.endDate as Date);
              const barWidth = Math.max(6, xEnd - xStart);
              const yTop = toY(Math.min(r.f, r.e));
              const barHeight = Math.max(8, Math.abs(r.e - r.f) * pxPerMeter);
              const fill = fillForStatus((r.plan_status || "").trim(), terminal);
              const textColor = textColorForFill(fill);
              const label = (r.voyage || r.vessel || "").toString();
              const tooltip = [
                `${terminal}-${r.berth ?? ""}`,
                `Vessel: ${r.vessel ?? "-"}`,
                `Voyage: ${r.voyage ?? "-"}`,
                `Pilot: ${r.pilot ?? "-"}`,
                `Start: ${r.startDate?.toLocaleString() ?? "-"}`,
                `End: ${r.endDate?.toLocaleString() ?? "-"}`,
                `F: ${Number.isFinite(r.f) ? r.f.toFixed(0) : "-"}`,
                `E: ${Number.isFinite(r.e) ? r.e.toFixed(0) : "-"}`,
                r.note ? `Note: ${r.note}` : "",
              ]
                .filter(Boolean)
                .join(" | ");

              return (
                <g key={`bar-${terminal}-${r.row_id}`}>
                  <rect
                    x={xStart}
                    y={yTop}
                    width={barWidth}
                    height={barHeight}
                    fill={fill}
                    stroke="rgba(25,25,25,0.45)"
                    strokeWidth={1}
                    rx={4}
                    ry={4}
                    cursor={disabled ? "default" : "grab"}
                    onPointerDown={onDragStart(r)}
                  >
                    <title>{tooltip}</title>
                  </rect>
                  <text
                    x={xStart + barWidth / 2}
                    y={yTop + barHeight / 2}
                    textAnchor="middle"
                    dominantBaseline="middle"
                    fontSize={12}
                    fill={textColor}
                    pointerEvents="none"
                  >
                    {label || `#${r.row_id}`}
                  </text>
                </g>
              );
            })}
          </svg>
        </div>
      </div>
    );
  };

  const legend = (
    <div className="legend">
      {Object.entries(STATUS_COLORS).map(([k, v]) => (
        <span key={k} className="legend-chip">
          <span className="chip-color" style={{ background: v }} />
          <span className="chip-label">{k}</span>
        </span>
      ))}
      <span className="legend-chip">
        <span className="chip-color" style={{ background: "rgba(120,160,240,0.78)" }} />
        <span className="chip-label">SND fallback</span>
      </span>
      <span className="legend-chip">
        <span className="chip-color" style={{ background: "rgba(240,180,80,0.78)" }} />
        <span className="chip-label">GAM fallback</span>
      </span>
    </div>
  );

  return (
    <div
      ref={rootRef}
      className="timeline-root"
      data-disabled={disabled ? "1" : "0"}
      style={{
        background: theme?.base === "dark" ? "linear-gradient(180deg,#141720,#181c28)" : undefined,
        color: theme?.textColor || "#111",
      }}
    >
      <div className="timeline-header">
        <div>
          <div className="title">React Drag & Drop Timeline</div>
          <div className="subtitle">
            Drag horizontally (5 min snap) or vertically (30 m snap). Range stays fixed during drag,
            and one payload is emitted once on drop.
          </div>
        </div>
        {legend}
      </div>
      {booting && (
        <div className="timeline-loading-banner" aria-live="polite">
          <span className="timeline-spinner" />
          <div>
            <div className="timeline-loading-title">React 편집기를 준비하는 중입니다</div>
            <div className="timeline-loading-sub">처음 1회 또는 데이터가 바뀐 직후에는 잠시 시간이 걸릴 수 있습니다.</div>
          </div>
        </div>
      )}
      {renderTerminal("SND")}
      {renderTerminal("GAM")}
    </div>
  );
};

export default Timeline;
