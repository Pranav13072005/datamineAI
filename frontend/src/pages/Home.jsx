import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Upload as UploadIcon } from 'lucide-react';
import { FileUpload, DatasetList } from '../components';
import { useDatasets } from '../hooks/useDatasets';
import { MAX_UPLOAD_MB } from '../utils/config';

export default function Home() {
  const navigate = useNavigate();
  const { datasets, loading, error, fetchDatasets, uploadDataset, deleteDataset } = useDatasets();
  const [selectedDataset, setSelectedDataset] = useState(null);
  const [uploadError, setUploadError] = useState(null);
  const [uploadSuccess, setUploadSuccess] = useState(false);

  useEffect(() => {
    fetchDatasets();
  }, []);

  const handleFileSelect = async (file) => {
    setUploadError(null);
    setUploadSuccess(false);

    const formData = new FormData();
    formData.append('file', file);

    try {
      const uploadedDataset = await uploadDataset(formData);
      setUploadSuccess(true);
      setSelectedDataset(uploadedDataset);
      setTimeout(() => setUploadSuccess(false), 3000);
    } catch (err) {
      setUploadError(err.message || 'Failed to upload dataset');
    }
  };

  const handleSelectDataset = (dataset) => {
    setSelectedDataset(dataset);
  };

  const handleStartAnalysis = () => {
    if (selectedDataset) {
      navigate(`/dashboard/${selectedDataset.id}`);
    }
  };

  const handleDeleteDataset = async (datasetId) => {
    if (window.confirm('Are you sure you want to delete this dataset?')) {
      await deleteDataset(datasetId);
      if (selectedDataset?.id === datasetId) {
        setSelectedDataset(null);
      }
    }
  };

  return (
    <div className="min-h-screen bg-[#020617] text-slate-200">
      {/* Background Accents */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none z-0">
        <div className="absolute top-[-10%] right-[-5%] w-[800px] h-[800px] bg-blue-600/10 rounded-full blur-[120px]"></div>
        <div className="absolute bottom-[-10%] left-[-5%] w-[600px] h-[600px] bg-purple-600/10 rounded-full blur-[100px]"></div>
        <div className="absolute top-[30%] left-[20%] w-[500px] h-[500px] bg-cyan-600/5 rounded-full blur-[90px]"></div>
      </div>

      <div className="relative max-w-7xl mx-auto px-4 py-12">
        {/* Header */}
        <div className="mb-16 text-center slide-in">
          <div className="flex items-center justify-center gap-3 mb-6">
            <div className="p-3 rounded-lg bg-gradient-to-br from-blue-500/20 to-purple-500/20">
              <UploadIcon className="w-8 h-8 text-blue-400" />
            </div>
            <h1 className="text-5xl font-bold bg-gradient-to-r from-blue-400 via-cyan-300 to-purple-400 bg-clip-text text-transparent">
              AI Data Analyst
            </h1>
          </div>
          <p className="text-slate-300 text-lg max-w-2xl mx-auto">
            Upload your datasets and ask questions in natural language to uncover insights instantly powered by AI
          </p>
        </div>

        {/* Error Messages */}
        {error && (
          <div className="mb-6 alert-error fade-in flex items-center gap-3">
            <span className="text-xl">⚠️</span>
            <div>
              <p className="font-semibold">Loading Error</p>
              <p className="text-sm">{error}</p>
            </div>
          </div>
        )}
        {uploadError && (
          <div className="mb-6 alert-error fade-in flex items-center gap-3">
            <span className="text-xl">❌</span>
            <div>
              <p className="font-semibold">Upload Error</p>
              <p className="text-sm">{uploadError}</p>
            </div>
          </div>
        )}
        {uploadSuccess && (
          <div className="mb-6 alert-success fade-in flex items-center gap-3">
            <span className="text-xl">✓</span>
            <div>
              <p className="font-semibold">Success!</p>
              <p className="text-sm">Dataset uploaded successfully! Ready for analysis.</p>
            </div>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 mb-12">
          {/* Upload Section */}
          <div className="lg:col-span-2 slide-in-left">
            <div className="card-elevated">
              <div className="flex items-center gap-2 mb-6">
                <div className="w-3 h-3 rounded-full bg-blue-400"></div>
                <h2 className="text-2xl font-bold text-white">Upload Dataset</h2>
              </div>
              <FileUpload
                onFileSelect={handleFileSelect}
                loading={loading}
              />
              <div className="mt-8 p-4 bg-gradient-to-r from-blue-500/10 to-cyan-500/10 border border-blue-500/30 rounded-lg">
                <h4 className="font-semibold text-blue-300 mb-3 flex items-center gap-2">
                  <span>📝</span> Supported Formats & Limits
                </h4>
                <ul className="text-sm text-blue-200 space-y-2">
                  <li className="flex items-center gap-2">
                    <span className="w-1.5 h-1.5 rounded-full bg-blue-400"></span>
                    CSV (.csv)
                  </li>
                  <li className="flex items-center gap-2">
                    <span className="w-1.5 h-1.5 rounded-full bg-blue-400"></span>
                    Excel Workbook (.xlsx, .xls)
                  </li>
                  <li className="flex items-center gap-2">
                    <span className="w-1.5 h-1.5 rounded-full bg-blue-400"></span>
                    Max file size: {MAX_UPLOAD_MB}MB
                  </li>
                </ul>
              </div>
            </div>
          </div>

          {/* Quick Start */}
          <div className="slide-in-right">
            <div className="card-elevated h-full">
              <div className="flex items-center gap-2 mb-6">
                <div className="w-3 h-3 rounded-full bg-purple-400"></div>
                <h2 className="text-2xl font-bold text-white">Quick Start</h2>
              </div>
              <div className="space-y-3">
                <div className="p-4 bg-slate-700/40 border border-slate-600/40 rounded-lg hover:border-slate-500/60 transition-all">
                  <p className="text-sm text-slate-300">
                    <span className="font-bold text-blue-400">1</span>
                    <span className="ml-3">Upload a CSV or Excel file</span>
                  </p>
                </div>
                <div className="p-4 bg-slate-700/40 border border-slate-600/40 rounded-lg hover:border-slate-500/60 transition-all">
                  <p className="text-sm text-slate-300">
                    <span className="font-bold text-blue-400">2</span>
                    <span className="ml-3">Select a dataset from the list</span>
                  </p>
                </div>
                <div className="p-4 bg-slate-700/40 border border-slate-600/40 rounded-lg hover:border-slate-500/60 transition-all">
                  <p className="text-sm text-slate-300">
                    <span className="font-bold text-blue-400">3</span>
                    <span className="ml-3">Click Analyze to start</span>
                  </p>
                </div>
                <div className="p-4 bg-slate-700/40 border border-slate-600/40 rounded-lg hover:border-slate-500/60 transition-all">
                  <p className="text-sm text-slate-300">
                    <span className="font-bold text-blue-400">4</span>
                    <span className="ml-3">Ask questions about your data</span>
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Datasets Section */}
        <div className="fade-in">
          <div className="card-elevated">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full bg-emerald-400"></div>
                <h2 className="text-2xl font-bold text-white">Available Datasets</h2>
              </div>
              <span className="px-3 py-1 bg-slate-700/50 text-slate-300 text-sm rounded-full font-medium">
                {datasets.length} dataset{datasets.length !== 1 ? 's' : ''}
              </span>
            </div>

            <DatasetList
              datasets={datasets}
              selectedDataset={selectedDataset}
              onSelect={handleSelectDataset}
              onDelete={handleDeleteDataset}
              loading={loading}
            />

            {selectedDataset && (
              <div className="mt-8 flex gap-3 slide-in">
                <button
                  onClick={handleStartAnalysis}
                  className="btn-primary flex-1 py-3 flex items-center justify-center gap-2 text-base font-semibold"
                >
                  <span>📊</span> Analyze Selected Dataset
                </button>
                <button
                  onClick={() => setSelectedDataset(null)}
                  className="btn-secondary py-3 px-6"
                >
                  ✕ Clear
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
