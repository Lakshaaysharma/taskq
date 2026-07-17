import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // The Redis client is a Node-only dependency; keep it out of the browser bundle.
  serverExternalPackages: ["redis"],
};

export default nextConfig;
