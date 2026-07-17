import { NextRequest, NextResponse } from "next/server";
import { getQueueSummary, getQueueTasks } from "@/lib/queue";

export const dynamic = "force-dynamic";

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ name: string }> },
) {
  try {
    const { name } = await params;
    const [summary, tasks] = await Promise.all([
      getQueueSummary(name),
      getQueueTasks(name),
    ]);
    return NextResponse.json({ summary, tasks });
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : "unknown error" },
      { status: 500 },
    );
  }
}
