import { clsx } from 'clsx';

const variants = {
  default: 'bg-gray-100 text-gray-800',
  success: 'bg-green-100 text-green-800',
  warning: 'bg-yellow-100 text-yellow-800',
  error: 'bg-red-100 text-red-800',
  info: 'bg-blue-100 text-blue-800',
  credit: 'bg-emerald-100 text-emerald-800',
  debit: 'bg-rose-100 text-rose-800',
  insert: 'bg-green-100 text-green-800',
  update: 'bg-blue-100 text-blue-800',
  delete: 'bg-red-100 text-red-800',
  replace: 'bg-purple-100 text-purple-800',
};

const sizes = {
  sm: 'px-2 py-0.5 text-xs',
  md: 'px-2.5 py-1 text-sm',
  lg: 'px-3 py-1.5 text-base',
};

export function Badge({
  children,
  variant = 'default',
  size = 'sm',
  className,
  dot = false,
  ...props
}) {
  return (
    <span
      className={clsx(
        'inline-flex items-center font-medium rounded-full',
        variants[variant] || variants.default,
        sizes[size],
        className
      )}
      {...props}
    >
      {dot && (
        <span
          className={clsx(
            'w-1.5 h-1.5 rounded-full mr-1.5',
            variant === 'success' && 'bg-green-500',
            variant === 'error' && 'bg-red-500',
            variant === 'warning' && 'bg-yellow-500',
            variant === 'info' && 'bg-blue-500',
            !['success', 'error', 'warning', 'info'].includes(variant) && 'bg-gray-500'
          )}
        />
      )}
      {children}
    </span>
  );
}

export default Badge;
