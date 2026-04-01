import { useState, useRef, useEffect } from 'react';
import { Send } from 'lucide-react';

export default function QueryBox({ onSubmit, loading = false, disabled = false }) {
  const [question, setQuestion] = useState('');
  const textareaRef = useRef(null);

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 120) + 'px';
    }
  }, [question]);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (question.trim() && !loading && !disabled) {
      onSubmit(question);
      setQuestion('');
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && e.ctrlKey) {
      handleSubmit(e);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="w-full">
      <div className="relative group">
        <textarea
          ref={textareaRef}
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask a question about your data... (Ctrl+Enter to send)"
          disabled={loading || disabled}
          className="input-field pr-14 resize-none max-h-[140px] w-full"
          rows="1"
        />
        <button
          type="submit"
          disabled={loading || disabled || !question.trim()}
          className="absolute right-3 bottom-3 p-2.5 bg-gradient-to-br from-blue-600 to-blue-700 text-white rounded-lg hover:from-blue-500 hover:to-blue-600 disabled:bg-slate-600 disabled:cursor-not-allowed transition-all hover:scale-110 active:scale-95 shadow-lg group-focus-within:shadow-blue-500/50"
          title="Send query (Ctrl+Enter)"
        >
          {loading ? (
            <div className="animate-spin text-lg">⏳</div>
          ) : (
            <Send className="w-5 h-5" />
          )}
        </button>
      </div>
      <p className="text-xs text-slate-500 mt-3 flex items-center gap-1">
        <span>💡</span>
        <span>Try: "What is the average X?" or "Show me top 5 Y by Z"</span>
      </p>
    </form>
  );
}
