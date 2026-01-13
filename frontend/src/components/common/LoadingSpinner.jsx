import { clsx } from 'clsx';

export function LoadingSpinner({ size = 'md', className }) {
  const sizes = {
    sm: 'w-4 h-4 border-2',
    md: 'w-6 h-6 border-2',
    lg: 'w-8 h-8 border-3',
    xl: 'w-12 h-12 border-4',
  };

  return (
    <div
      className={clsx(
        'animate-spin rounded-full border-gray-300 border-t-mongodb-green',
        sizes[size],
        className
      )}
    />
  );
}

export function LoadingOverlay({ message = 'Loading...' }) {
  return (
    <div className="absolute inset-0 bg-white/80 flex items-center justify-center z-10">
      <div className="flex flex-col items-center gap-2">
        <LoadingSpinner size="lg" />
        <span className="text-sm text-gray-600">{message}</span>
      </div>
    </div>
  );
}

export default LoadingSpinner;
