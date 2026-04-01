import { useState, useRef } from 'react';
import { Upload, X } from 'lucide-react';

export default function FileUpload({ onFileSelect, loading = false }) {
  const [isDragging, setIsDragging] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const fileInputRef = useRef(null);

  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);

    const files = e.dataTransfer.files;
    if (files.length > 0) {
      processFile(files[0]);
    }
  };

  const handleFileInputChange = (e) => {
    if (e.target.files && e.target.files.length > 0) {
      processFile(e.target.files[0]);
    }
  };

  const processFile = (file) => {
    // Validate file type (CSV or Excel)
    const validTypes = ['text/csv', 'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'];
    if (!validTypes.includes(file.type) && !file.name.match(/\.(csv|xlsx|xls)$/i)) {
      alert('Please select a valid CSV or Excel file');
      return;
    }

    setSelectedFile(file);
    onFileSelect(file);
  };

  const handleRemoveFile = () => {
    setSelectedFile(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  return (
    <div className="w-full">
      <div
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        className={`relative border-2 border-dashed rounded-xl p-8 transition-all cursor-pointer backdrop-filter backdrop-blur-sm ${
          isDragging
            ? 'border-blue-500 bg-blue-500/15 scale-105'
            : 'border-slate-600 bg-slate-800/30 hover:border-blue-400 hover:bg-blue-500/10'
        }`}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.xlsx,.xls"
          onChange={handleFileInputChange}
          className="hidden"
          disabled={loading}
        />

        <div
          onClick={() => !loading && fileInputRef.current?.click()}
          className="flex flex-col items-center justify-center py-8"
        >
          <div className="p-4 bg-gradient-to-br from-blue-500/20 to-cyan-500/20 rounded-xl mb-4 group-hover:scale-110 transition-transform">
            <Upload className="w-12 h-12 text-blue-400" />
          </div>
          <h3 className="text-lg font-bold text-white mb-2">
            Drag & drop your file
          </h3>
          <p className="text-slate-400 mb-4 font-medium">or click to browse</p>
          <p className="text-xs text-slate-500">
            CSV, Excel (.xlsx, .xls) • Max 50MB
          </p>
        </div>
      </div>

      {selectedFile && !loading && (
        <div className="mt-5 p-4 bg-gradient-to-r from-slate-700/50 to-slate-600/30 rounded-lg border border-slate-600/50 flex items-center justify-between hover:border-slate-500 transition-all">
          <div>
            <p className="font-semibold text-white">{selectedFile.name}</p>
            <p className="text-sm text-slate-400">
              {(selectedFile.size / 1024 / 1024).toFixed(2)} MB
            </p>
          </div>
          <button
            onClick={handleRemoveFile}
            className="p-2 hover:bg-red-500/20 rounded-lg transition-all text-red-400 hover:text-red-300"
          >
            <X className="w-5 h-5" />
          </button>
        </div>
      )}

      {loading && (
        <div className="mt-4 p-4 bg-slate-800 rounded-lg border border-slate-700">
          <p className="text-slate-300">Uploading...</p>
        </div>
      )}
    </div>
  );
}
