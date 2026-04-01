import { Trash2, Database } from 'lucide-react';

export default function DatasetList({
  datasets = [],
  selectedDataset = null,
  onSelect,
  onDelete,
  loading = false,
}) {
  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="relative w-10 h-10">
          <div className="absolute inset-0 rounded-full bg-gradient-to-r from-blue-500 to-purple-500 opacity-20 animate-pulse"></div>
          <div className="absolute inset-2 rounded-full border-3 border-transparent border-t-blue-400 border-r-blue-400 animate-spin"></div>
        </div>
      </div>
    );
  }

  if (datasets.length === 0) {
    return (
      <div className="text-center py-12">
        <Database className="w-14 h-14 text-slate-500 mx-auto mb-4 opacity-50" />
        <p className="text-slate-400 font-medium">No datasets uploaded yet</p>
        <p className="text-slate-500 text-sm mt-1">Upload a file above to get started</p>
      </div>
    );
  }

  return (
    <div className="space-y-3 max-h-96 overflow-y-auto pr-2 custom-scrollbar">
      {datasets.map((dataset) => (
        <div
          key={dataset.id}
          onClick={() => onSelect(dataset)}
          className={`p-4 rounded-lg border-2 cursor-pointer transition-all transform hover:scale-102 ${
            selectedDataset?.id === dataset.id
              ? 'border-blue-500 bg-blue-500/15 shadow-lg shadow-blue-500/20'
              : 'border-slate-700 bg-slate-800/40 hover:border-slate-600 hover:bg-slate-800/60'
          }`}
        >
          <div className="flex items-center justify-between">
            <div className="flex-1">
              <h4 className="font-bold text-white">{dataset.name}</h4>
              <div className="flex gap-4 text-xs text-slate-400 mt-2 flex-wrap">
                <span className="flex items-center gap-1">
                  <span>📊</span>
                  {dataset.rows || 0} rows
                </span>
                <span className="flex items-center gap-1">
                  <span>📋</span>
                  {dataset.columns || 0} columns
                </span>
                {dataset.uploaded_at && (
                  <span className="flex items-center gap-1">
                    <span>⏰</span>
                    {new Date(dataset.uploaded_at).toLocaleDateString()}
                  </span>
                )}
              </div>
            </div>
            {selectedDataset?.id === dataset.id && (
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onDelete(dataset.id);
                }}
                className="p-2 hover:bg-red-500/20 text-red-400 hover:text-red-300 rounded-lg transition-all ml-4"
                title="Delete dataset"
              >
                <Trash2 className="w-5 h-5" />
              </button>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
