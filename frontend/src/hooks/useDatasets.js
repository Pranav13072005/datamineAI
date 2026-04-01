import { useState, useCallback } from 'react';
import { datasetAPI } from '../services/api';

export const useDatasets = () => {
  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchDatasets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await datasetAPI.getAll();
      setDatasets(response.data || []);
    } catch (err) {
      setError(err.message || 'Failed to fetch datasets');
      setDatasets([]);
    } finally {
      setLoading(false);
    }
  }, []);

  const uploadDataset = useCallback(async (formData) => {
    setLoading(true);
    setError(null);
    try {
      const response = await datasetAPI.upload(formData);
      setDatasets([...datasets, response.data]);
      return response.data;
    } catch (err) {
      setError(err.message || 'Failed to upload dataset');
      throw err;
    } finally {
      setLoading(false);
    }
  }, [datasets]);

  const deleteDataset = useCallback(async (datasetId) => {
    setLoading(true);
    setError(null);
    try {
      await datasetAPI.delete(datasetId);
      setDatasets(datasets.filter((d) => d.id !== datasetId));
    } catch (err) {
      setError(err.message || 'Failed to delete dataset');
      throw err;
    } finally {
      setLoading(false);
    }
  }, [datasets]);

  return {
    datasets,
    loading,
    error,
    fetchDatasets,
    uploadDataset,
    deleteDataset,
  };
};
