/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        mongodb: {
          green: '#00ED64',
          darkgreen: '#001E2B',
          leaf: '#00684A',
          spring: '#E3FCF7',
          forest: '#023430',
          mist: '#E8EDEB',
          slate: '#1C2D38',
        },
        databricks: {
          orange: '#FF3621',
          red: '#DB1A1A',
        }
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'flow': 'flow 2s ease-in-out infinite',
      },
      keyframes: {
        flow: {
          '0%, 100%': { transform: 'translateX(0)' },
          '50%': { transform: 'translateX(10px)' },
        }
      }
    },
  },
  plugins: [],
}
