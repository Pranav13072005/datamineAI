import { useState, useCallback } from 'react';
import { datasetAPI } from '../services/api';

const getErrorMessage = (err, fallback) => {
  // Axios-style errors
  const detail = err?.response?.data?.detail;
  if (typeof detail === 'string' && detail.trim()) return detail;
  const message = err?.message;
  if (typeof message === 'string' && message.trim()) return message;
  return fallback;
};

export const useDatasets = () => {
  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchDatasets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await datasetAPI.getAll();
      const list = response.data || [];
      setDatasets(list);
      return list;
    } catch (err) {
      setError(getErrorMessage(err, 'Failed to fetch datasets'));
      setDatasets([]);
      return [];
    } finally {
      setLoading(false);
    }
  }, []);

  const uploadDataset = useCallback(async (formData) => {
    setLoading(true);
    try {
      const response = await datasetAPI.upload(formData);
      // Backend returns: { dataset_id, filename, row_count, columns: string[], message }
      // UI expects: { id, name, rows, columns }
      const payload = response.data;
      const mapped = {
        id: payload.dataset_id,
        name: payload.filename,
        rows: payload.row_count,
        columns: Array.isArray(payload.columns) ? payload.columns.length : 0,
        uploaded_at: new Date().toISOString(),
      };
      setDatasets((prev) => [mapped, ...prev]);
      return mapped;
    } catch (err) {
      throw new Error(getErrorMessage(err, 'Failed to upload dataset'));
    } finally {
      setLoading(false);
    }
  }, []);

  const deleteDataset = useCallback(async (datasetId) => {
    setLoading(true);
    try {
      await datasetAPI.delete(datasetId);
      setDatasets((prev) => prev.filter((d) => d.id !== datasetId));
    } catch (err) {
      throw new Error(getErrorMessage(err, 'Failed to delete dataset'));
    } finally {
      setLoading(false);
    }
  }, []);

  return {
    datasets,
    loading,
    error,
    fetchDatasets,
    uploadDataset,
    deleteDataset,
  };
};
