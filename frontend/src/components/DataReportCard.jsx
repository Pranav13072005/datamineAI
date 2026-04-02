export default function DataReportCard({
  title = 'Your data at a glance',
  status,
  shape,
  missing,
  highMissing,
  correlations,
  distributionFlags,
  duplicates,
  errorMessage,
  timeoutMessage,
  generatedAt,
}) {
  const rows = shape?.rows;
  const cols = shape?.cols;

  const isTimedOut = Boolean(timeoutMessage) && status !== 'ready' && status !== 'error';
  const effectiveStatus = isTimedOut ? 'timeout' : status;

  const showReady = effectiveStatus === 'ready';
  const showProcessing = effectiveStatus === 'processing';
  const showError = effectiveStatus === 'error';
  const showTimeout = effectiveStatus === 'timeout';

  const pillClassForFlag = (flag) => {
    switch (flag) {
      case 'high_skew':
        return 'bg-amber-500/10 border border-amber-500/20 text-amber-300';
      case 'possible_id':
        return 'bg-blue-500/10 border border-blue-500/20 text-blue-300';
      case 'constant':
        return 'bg-red-500/10 border border-red-500/20 text-red-300';
      default:
        return 'bg-slate-700/40 border border-slate-600/40 text-slate-300';
    }
  };

  const duplicatesPct = typeof duplicates?.pct === 'number' ? duplicates.pct : null;
  const showDuplicateWarning = typeof duplicatesPct === 'number' && duplicatesPct > 1;

  const formatPct = (value, digits = 2) => {
    if (typeof value !== 'number' || Number.isNaN(value)) return '—';
    return `${value.toFixed(digits)}%`;
  };

  const missingColsCount = Array.isArray(missing) ? missing.length : 0;
  const highMissingColsCount = Array.isArray(highMissing) ? highMissing.length : 0;
  const showSummaryRow = showReady && (missingColsCount > 0 || duplicatesPct !== null);

  return (
    <div className="card-elevated">
      <div className="flex items-start justify-between gap-3 mb-4">
        <div>
          <h2 className="text-lg font-bold text-white">{title}</h2>
          <p className="text-xs text-slate-400 mt-1">
            {Number.isFinite(rows) && Number.isFinite(cols)
              ? `${rows.toLocaleString()} rows • ${cols.toLocaleString()} cols`
              : 'Generating summary...'}
          </p>
          {generatedAt instanceof Date && !Number.isNaN(generatedAt.getTime()) ? (
            <p className="text-[11px] text-slate-500 mt-1">Generated {generatedAt.toLocaleString()}</p>
          ) : null}
        </div>

        {showProcessing ? (
          <span className="px-2.5 py-1 rounded-full text-xs font-semibold bg-blue-500/10 border border-blue-500/20 text-blue-300">
            Processing
          </span>
        ) : null}
        {showTimeout ? (
          <span className="px-2.5 py-1 rounded-full text-xs font-semibold bg-amber-500/10 border border-amber-500/20 text-amber-300">
            Timed out
          </span>
        ) : null}
        {showReady ? (
          <span className="px-2.5 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 border border-emerald-500/20 text-emerald-300">
            Ready
          </span>
        ) : null}
        {showError ? (
          <span className="px-2.5 py-1 rounded-full text-xs font-semibold bg-red-500/10 border border-red-500/20 text-red-300">
            Error
          </span>
        ) : null}
      </div>

      {timeoutMessage ? (
        <div className="mb-4 p-3 bg-amber-500/10 border border-amber-500/20 rounded-lg text-amber-200 text-sm">
          {timeoutMessage}
        </div>
      ) : null}

      {showError && errorMessage ? (
        <div className="mb-4 p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-red-200 text-sm">
          {errorMessage}
        </div>
      ) : null}

      {showProcessing && !timeoutMessage ? (
        <div className="text-sm text-slate-300">
          Computing quick insights... this usually takes a few seconds.
        </div>
      ) : null}

      {showReady ? (
        <div className="space-y-4">
          {showSummaryRow ? (
            <div className="flex items-start justify-between gap-3 p-3 bg-slate-800/30 border border-slate-700/40 rounded-lg">
              <div className="text-xs text-slate-300 leading-5">
                <span className="text-slate-400">Duplicates:</span>{' '}
                <span className="font-semibold text-slate-200">
                  {Number(duplicates?.count || 0).toLocaleString()} ({formatPct(duplicatesPct)})
                </span>
                <span className="text-slate-500"> • </span>
                <span className="text-slate-400">Missing cols:</span>{' '}
                <span className="font-semibold text-slate-200">{missingColsCount.toLocaleString()}</span>
                {highMissingColsCount > 0 ? (
                  <>
                    <span className="text-slate-500"> • </span>
                    <span className="text-amber-300 font-semibold">{highMissingColsCount.toLocaleString()} high</span>
                  </>
                ) : null}
              </div>

              <span
                className={
                  'px-2.5 py-1 rounded-full text-[11px] font-semibold border ' +
                  (showDuplicateWarning
                    ? 'bg-amber-500/10 border-amber-500/20 text-amber-300'
                    : missingColsCount > 0
                      ? 'bg-slate-700/40 border-slate-600/40 text-slate-200'
                      : 'bg-emerald-500/10 border-emerald-500/20 text-emerald-300')
                }
              >
                {showDuplicateWarning ? 'Needs attention' : missingColsCount > 0 ? 'Review missing' : 'Looks good'}
              </span>
            </div>
          ) : null}

          {showDuplicateWarning ? (
            <div className="p-3 bg-gradient-to-r from-yellow-500/10 to-amber-500/10 border-l-4 border-yellow-400 rounded-lg">
              <p className="text-sm text-slate-200 leading-5">
                <span className="font-semibold">Duplicate rows detected:</span>{' '}
                {Number(duplicates?.count || 0).toLocaleString()} ({duplicatesPct.toFixed(2)}%)
              </p>
            </div>
          ) : null}

          {Array.isArray(missing) && missing.length > 0 ? (
            <div>
              <h3 className="text-sm font-bold text-white mb-2">Missing values</h3>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-slate-400 border-b border-slate-700/60">
                      <th className="text-left py-2">Column</th>
                      <th className="text-right py-2">Missing</th>
                      <th className="text-right py-2">Pct</th>
                    </tr>
                  </thead>
                  <tbody>
                    {missing.slice(0, 5).map((m) => (
                      <tr key={m.column} className="border-b border-slate-800/60">
                        <td className="py-2 text-slate-200 truncate max-w-[160px]" title={m.column}>
                          {m.column}
                        </td>
                        <td className="py-2 text-right text-slate-300">{Number(m.count).toLocaleString()}</td>
                        <td className="py-2 text-right text-slate-300">
                          {typeof m.pct === 'number' ? `${m.pct.toFixed(1)}%` : ''}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {missing.length > 5 ? (
                <p className="text-[11px] text-slate-500 mt-2">Showing top 5 of {missing.length} columns</p>
              ) : null}

              {Array.isArray(highMissing) && highMissing.length > 0 ? (
                <div className="mt-3">
                  <p className="text-xs text-slate-400 mb-2">High missing (&gt;30%)</p>
                  <div className="flex flex-wrap gap-2">
                    {highMissing.slice(0, 8).map((c) => (
                      <span
                        key={c}
                        className="px-2.5 py-1 rounded-full text-xs font-semibold bg-amber-500/10 border border-amber-500/20 text-amber-300"
                        title={c}
                      >
                        {c}
                      </span>
                    ))}
                    {highMissing.length > 8 ? (
                      <span className="px-2.5 py-1 rounded-full text-xs font-semibold bg-slate-700/40 border border-slate-600/40 text-slate-300">
                        +{highMissing.length - 8} more
                      </span>
                    ) : null}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}

          {Array.isArray(correlations) && correlations.length > 0 ? (
            <div>
              <h3 className="text-sm font-bold text-white mb-2">Top relationships found</h3>
              <div className="space-y-2">
                {correlations.slice(0, 4).map((c, idx) => (
                  <div
                    key={`${c.col_a}-${c.col_b}-${idx}`}
                    className="px-3 py-2 bg-slate-700/30 border border-slate-600/40 rounded-lg"
                  >
                    <p className="text-sm text-slate-200 leading-5">
                      <span className="font-semibold">{c.col_a}</span> vs{' '}
                      <span className="font-semibold">{c.col_b}</span>
                      <span className="text-slate-400"> • r={typeof c.r === 'number' ? c.r.toFixed(2) : c.r}</span>
                    </p>
                  </div>
                ))}
              </div>

              {correlations.length > 4 ? (
                <p className="text-[11px] text-slate-500 mt-2">Showing top 4 of {correlations.length} pairs</p>
              ) : null}
            </div>
          ) : null}

          {Array.isArray(distributionFlags) && distributionFlags.length > 0 ? (
            <div>
              <h3 className="text-sm font-bold text-white mb-2">Flags</h3>
              <div className="flex flex-wrap gap-2">
                {distributionFlags.slice(0, 10).map((f, idx) => (
                  <span
                    key={`${f.column}-${f.flag}-${idx}`}
                    className={`px-2.5 py-1 rounded-full text-xs font-semibold ${pillClassForFlag(f.flag)}`}
                    title={f.detail ? JSON.stringify(f.detail) : ''}
                  >
                    {f.column}: {f.flag}
                  </span>
                ))}

                {distributionFlags.length > 10 ? (
                  <span className="px-2.5 py-1 rounded-full text-xs font-semibold bg-slate-700/40 border border-slate-600/40 text-slate-300">
                    +{distributionFlags.length - 10} more
                  </span>
                ) : null}
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
