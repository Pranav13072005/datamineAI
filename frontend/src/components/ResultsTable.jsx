import { ChevronLeft, ChevronRight } from 'lucide-react';
import { useState } from 'react';

export default function ResultsTable({ data = [], title = 'Results' }) {
  const [currentPage, setCurrentPage] = useState(0);
  const itemsPerPage = 10;

  if (!data || data.length === 0) {
    return (
      <div className="text-center py-8">
        <p className="text-slate-400">No data to display</p>
      </div>
    );
  }

  // Handle both array of objects and array of arrays
  let rows = data;
  let columns = [];

  if (rows.length > 0) {
    if (typeof rows[0] === 'object' && !Array.isArray(rows[0])) {
      // Array of objects
      columns = Object.keys(rows[0]);
    } else if (Array.isArray(rows[0])) {
      // Array of arrays - use first row as header if possible
      if (rows[0].every(item => typeof item === 'string')) {
        columns = rows[0];
        rows = rows.slice(1);
      } else {
        columns = rows[0].map((_, i) => `Column ${i + 1}`);
      }
    }
  }

  const totalPages = Math.ceil(rows.length / itemsPerPage);
  const paginatedRows = rows.slice(
    currentPage * itemsPerPage,
    (currentPage + 1) * itemsPerPage
  );

  const handlePrevPage = () => {
    setCurrentPage(Math.max(0, currentPage - 1));
  };

  const handleNextPage = () => {
    setCurrentPage(Math.min(totalPages - 1, currentPage + 1));
  };

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-emerald-400"></div>
          <h3 className="text-lg font-bold text-white">{title}</h3>
        </div>
        {totalPages > 1 && (
          <span className="text-xs px-2 py-1 bg-slate-700/50 text-slate-400 rounded">
            Page {currentPage + 1} of {totalPages}
          </span>
        )}
      </div>
      
      <div className="overflow-x-auto rounded-lg border border-slate-700/50">
        <table className="w-full text-sm">
          <thead className="bg-gradient-to-r from-slate-700/50 to-slate-600/30 border-b border-slate-700">
            <tr>
              {columns.map((col, idx) => (
                <th
                  key={idx}
                  className="px-4 py-4 text-left font-semibold text-slate-200 whitespace-nowrap bg-slate-700/30"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {paginatedRows.map((row, rowIdx) => (
              <tr key={rowIdx} className="hover:bg-slate-700/20 transition-colors">
                {columns.map((col, colIdx) => {
                  const value = Array.isArray(row) ? row[colIdx] : row[col];
                  return (
                    <td key={colIdx} className="px-4 py-3 text-slate-300">
                      {value !== null && value !== undefined ? String(value) : <span className="text-slate-500 italic">—</span>}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="mt-6 flex items-center justify-between">
          <p className="text-sm text-slate-400">
            Showing <span className="font-semibold text-slate-300">{currentPage * itemsPerPage + 1}</span> to <span className="font-semibold text-slate-300">{Math.min((currentPage + 1) * itemsPerPage, rows.length)}</span> of <span className="font-semibold text-slate-300">{rows.length}</span> rows
          </p>
          <div className="flex gap-2">
            <button
              onClick={handlePrevPage}
              disabled={currentPage === 0}
              className="p-2 hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed rounded transition-colors active:scale-95"
            >
              <ChevronLeft className="w-5 h-5 text-slate-400" />
            </button>
            <button
              onClick={handleNextPage}
              disabled={currentPage === totalPages - 1}
              className="p-2 hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed rounded transition-colors active:scale-95"
            >
              <ChevronRight className="w-5 h-5" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
