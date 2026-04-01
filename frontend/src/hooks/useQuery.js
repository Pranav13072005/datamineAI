import { useState, useCallback } from 'react';
import { queryAPI } from '../services/api';

export const useQuery = () => {
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const mapBackendResultToUI = useCallback((data) => {
    if (!data || typeof data !== 'object') return data;

    // If backend already returns structured fields, keep them.
    if (data.response || data.table_data || data.chart_data || data.insights) {
      return data;
    }

    const backendResult = data.result;

    // Table heuristic: list of objects.
    if (Array.isArray(backendResult) && backendResult.length > 0 && typeof backendResult[0] === 'object') {
      return {
        ...data,
        table_data: backendResult,
        table_title: 'Query Results',
      };
    }

    // Fallback: plain text / JSON.
    let responseText = '';
    if (typeof backendResult === 'string') {
      responseText = backendResult;
    } else {
      try {
        responseText = JSON.stringify(backendResult, null, 2);
      } catch {
        responseText = String(backendResult);
      }
    }

    return {
      ...data,
      response: responseText,
    };
  }, []);

  const executeQuery = useCallback(async (datasetId, question) => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const response = await queryAPI.execute(datasetId, question);
      const mapped = mapBackendResultToUI(response.data);
      setResult(mapped);
      return mapped;
    } catch (err) {
      const errorMessage = err.response?.data?.detail || err.message || 'Failed to execute query';
      setError(errorMessage);
      throw err;
    } finally {
      setLoading(false);
    }
  }, [mapBackendResultToUI]);

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
