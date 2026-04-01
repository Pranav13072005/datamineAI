import { useState, useCallback } from 'react';
import { queryAPI } from '../services/api';

export const useQuery = () => {
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const executeQuery = useCallback(async (datasetId, question) => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const response = await queryAPI.execute(datasetId, question);
      setResult(response.data);
      return response.data;
    } catch (err) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to execute query';
      setError(errorMessage);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const clearResult = useCallback(() => {
    setResult(null);
    setError(null);
  }, []);

  return {
    result,
    loading,
    error,
    executeQuery,
    clearResult,
  };
};
