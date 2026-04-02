import axios from 'axios';
import API_BASE_URL from '../utils/config';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Datasets API
export const datasetAPI = {
  // Upload a new dataset
  upload: (formData) => apiClient.post('/datasets/upload', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  }),

  // Get all datasets
  getAll: () => apiClient.get('/datasets'),

  // Get a specific dataset
  getById: (datasetId) => apiClient.get(`/datasets/${datasetId}`),

  // Get cached dataset insights
  getInsights: (datasetId) => apiClient.get(`/datasets/${datasetId}/insights`),

  // Delete a dataset
  delete: (datasetId) => apiClient.delete(`/datasets/${datasetId}`),
};

// Query API
export const queryAPI = {
  // Execute a query against a dataset
  execute: (datasetId, question) =>
    apiClient.post('/query', {
      dataset_id: datasetId,
      question: question,
    }),
};

// Export API
export const exportAPI = {
  // Export results as PDF
  exportPDF: (data) =>
    apiClient.post('/export/pdf', data, {
      responseType: 'blob',
    }),
};

// Health check
export const healthCheck = () => apiClient.get('/health');

export default apiClient;
