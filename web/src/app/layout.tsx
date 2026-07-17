import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "taskq dashboard",
  description: "Real-time monitoring for the taskq task queue",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
