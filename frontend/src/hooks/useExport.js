import { useCallback } from 'react';
import { exportAPI } from '../services/api';

export const useExport = () => {
  const exportToPDF = useCallback(async (data) => {
    try {
      const response = await exportAPI.exportPDF(data);
      const blob = response.data instanceof Blob
        ? response.data
        : new Blob([response.data], { type: 'application/pdf' });

      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `report-${Date.now()}.pdf`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
    } catch (err) {
      throw new Error(err.message || 'Failed to export PDF');
    }
  }, []);

  return { exportToPDF };
};
