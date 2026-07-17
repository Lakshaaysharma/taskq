import { NextResponse } from "next/server";
import { getAllSummaries } from "@/lib/queue";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const queues = await getAllSummaries();
    return NextResponse.json({ queues });
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : "unknown error" },
      { status: 500 },
    );
  }
}
