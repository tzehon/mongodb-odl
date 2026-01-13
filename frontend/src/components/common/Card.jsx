import { clsx } from 'clsx';

export function Card({ children, className, title, subtitle, action, ...props }) {
  return (
    <div
      className={clsx(
        'bg-white rounded-lg shadow-sm border border-gray-200',
        className
      )}
      {...props}
    >
      {(title || action) && (
        <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
          <div>
            {title && (
              <h3 className="text-sm font-semibold text-gray-900">{title}</h3>
            )}
            {subtitle && (
              <p className="text-xs text-gray-500 mt-0.5">{subtitle}</p>
            )}
          </div>
          {action && <div>{action}</div>}
        </div>
      )}
      <div className="p-4">{children}</div>
    </div>
  );
}

export default Card;
