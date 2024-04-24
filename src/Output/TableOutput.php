<?php

namespace Parallel\Output;

use Parallel\Helper\DataHelper;
use Parallel\Helper\TimeHelper;
use Parallel\TaskData;
use Parallel\TaskStack\StackedTask;
use Symfony\Component\Console\Formatter\OutputFormatterStyle;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Helper\Table as TableHelper;
use Symfony\Component\Console\Helper\TableSeparator;
use Symfony\Component\Console\Input\StringInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\ConsoleOutputInterface;
use Symfony\Component\Console\Output\ConsoleSectionOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

class TableOutput implements Output
{
    private ?int $doneTasksRows;
    private float $lastOverwrite = 0;

    private OutputInterface $output;
    private BufferedOutput $buffer;
    private SymfonyStyle $io;
    private ?ConsoleSectionOutput $section = null;

    private TableHelper $stackedTable;
    private TableHelper $mainTable;

    public function __construct(?int $doneTasksRows = null)
    {
        $this->doneTasksRows = $doneTasksRows;
    }

    public function setOutput(OutputInterface $output): void
    {
        $output->getFormatter()->setStyle('red', new OutputFormatterStyle('red'));
        $output->getFormatter()->setStyle('green', new OutputFormatterStyle('green'));
        $output->getFormatter()->setStyle('yellow', new OutputFormatterStyle('yellow'));

        $this->output = $output;
        $this->buffer = new BufferedOutput($output->getVerbosity(), $output->isDecorated(), $output->getFormatter());
        $this->io = new SymfonyStyle(new StringInput(''), $output);
        if ($output instanceof ConsoleOutputInterface) {
            $this->section = $output->section();
        }


        $this->mainTable = new TableHelper($this->buffer);
        $this->mainTable->setHeaders([
            'Task',
            'All',
            'OK',
            'SKP',
            'ERR',
            'WRN',
            'Progress',
            'Time',
            'Memory',
            'Message'
        ])->setColumnMaxWidth(9, 24);

        $this->stackedTable = new TableHelper($this->buffer);
        $this->stackedTable
            ->setHeaders(['Title', 'Waiting for']);
    }

    /**
     * @param OutputInterface $output
     */
    public function startMessage(): void
    {
        $this->io->info("Starting parallel task processing ...");
    }

    /**
     * @param OutputInterface $output
     * @param string $error
     */
    public function errorMessage(string $error): void
    {
        $this->io->error($error);
    }

    /**
     * @param array $data
     * @param float $elapsedTime
     */
    public function printToOutput(array $data, float $elapsedTime): void
    {
        if (microtime(true) - $this->lastOverwrite < 0.1) {
            return;
        }

        list($stacked, $running, $done) = $this->filterTasks($data);
        if ($this->output->isDebug()) {
            $this->renderStackedTable($stacked, $running);
        }
        $this->renderMainTable($data, $running, $done, $elapsedTime);

        if ($this->section !== null) {
            $this->section->overwrite($this->buffer->fetch() . "\n");
        } else {
            $this->output->writeln(["\033[2J\033[;H", $this->buffer->fetch()]);
        }
        $this->lastOverwrite = microtime(true);
    }

    /**
     * @param OutputInterface $output
     * @param array $data
     * @param float $duration
     */
    public function finishMessage(array $data, float $duration): void
    {
        $this->lastOverwrite = 0;
        $this->printToOutput($data, $duration);

        $this->io->success('Parallel task processing finished in ' . TimeHelper::formatTime($duration));
    }

    /**
     * @param TaskData[] $stacked
     * @param TaskData[] $running
     */
    private function renderStackedTable(array $stacked, array $running): void
    {
        if (!count($stacked)) {
            return;
        }

        $table = $this->stackedTable;
        $table->setRows([]);

        foreach ($stacked as $rowTitle => $row) {
            // Mark currently running tasks
            $waitingFor = [];
            foreach ($row->getStackedTask()->getCurrentRunAfter() as $runAfter) {
                if (in_array($runAfter, array_keys($running))) {
                    $waitingFor[] = '<green>' . $runAfter . '</green>';
                    continue;
                }
                $waitingFor[] = $runAfter;
            }

            $table->addRow([
                $this->formatTitle($rowTitle, $row),
                implode(' <yellow>|</yellow> ', $waitingFor)
            ]);
        }

        $table->render();
    }

    /**
     * @param OutputInterface $output
     * @param TaskData[] $all
     * @param TaskData[] $running
     * @param TaskData[] $done
     * @param float $elapsedTime
     */
    private function renderMainTable(
        array $all,
        array $running,
        array $done,
        float $elapsedTime
    ): void {

        $table = $this->mainTable;
        $table->setRows([]);

        $total = [
            'count' => 0,
            'success' => 0,
            'skip' => 0,
            'error' => 0,
            'code_errors' => 0,
            'duration' => 0
        ];

        $avgMemoryUsage = $this->getAvgMemoryUsage(array_merge($running, $done));
        $this->renderDoneTasks($table, $done, $avgMemoryUsage, $total);
        $this->renderRunningTasks($table, $running, $avgMemoryUsage, $total);

        $table->addRow([
            'Total (' . count($done) . '/' . count($all) . ')',
            number_format($total['count']),
            number_format($total['success']),
            number_format($total['skip']),
            number_format($total['error']),
            number_format($total['code_errors']),
            'Saved time: ' . TimeHelper::formatTime(max($total['duration'] - (int)$elapsedTime, 0)),
            TimeHelper::formatTime($elapsedTime),
            '',
            ''
        ]);

        $table->render();
    }

    /**
     * Filter tasks array
     * @param array $data
     * @return array
     */
    private function filterTasks(array $data): array
    {
        $done = $stacked = $running = [];
        foreach ($data as $taskTitle => $taskData) {
            if ($taskData->getStackedTask()->isInStatus(StackedTask::STATUS_DONE)) {
                $done[$taskTitle] = $taskData;
            } elseif ($taskData->getStackedTask()->isInStatus(StackedTask::STATUS_STACKED)) {
                $stacked[$taskTitle] = $taskData;
            } elseif ($taskData->getStackedTask()->isInStatus(StackedTask::STATUS_RUNNING)) {
                $running[$taskTitle] = $taskData;
            }
        }

        return [$stacked, $running, $done];
    }

    /**
     * @param TableHelper $table
     * @param TaskData[] $rows
     * @param int $avgMemoryUsage
     * @param array $total
     */
    private function renderRunningTasks(Table $table, array $rows, int $avgMemoryUsage, array &$total): void
    {
        foreach ($rows as $rowTitle => $row) {
            $table->addRow([
                $this->formatTitle($rowTitle, $row),
                number_format($row->getCount()),
                number_format($row->getExtra('success', 0)),
                number_format($row->getExtra('skip', 0)),
                number_format($row->getExtra('error', 0)),
                number_format($row->getCodeErrorsCount()),
                $this->progress($row->getProgress()),
                TimeHelper::formatTime($row->getDuration()) . '/' . TimeHelper::formatTime($row->getEstimated()),
                $this->formatMemory($row, $avgMemoryUsage),
                $row->getExtra('message', '')
            ]);

            $total['count'] += $row->getCount();
            $total['success'] += $row->getExtra('success', 0);
            $total['skip'] += $row->getExtra('skip', 0);
            $total['error'] += $row->getExtra('error', 0);
            $total['code_errors'] += $row->getCodeErrorsCount();
            $total['duration'] += $row->getDuration();
        }

        if (count($rows)) {
            $table->addRow(new TableSeparator());
        }
    }

    /**
     * @param TableHelper $table
     * @param TaskData[] $rows
     * @param int $avgMemoryUsage
     * @param array $total
     */
    private function renderDoneTasks(Table $table, array $rows, int $avgMemoryUsage, array &$total): void
    {
        $count = count($rows);
        $doneTasks = 0;

        foreach ($rows as $rowTitle => $row) {
            $rowMessage = $row->getStackedTask()->getFinishedAt() ? 'Finished at: ' . $row->getStackedTask()->getFinishedAt()->format('H:i:s') : '';
            if ($row->getExtra('error', 0) && $row->getExtra('message', '')) {
                $rowMessage .= ". " . $row->getExtra('message', '');
            }

            $isVisibleRow = $this->doneTasksRows === null || ($this->doneTasksRows > 0 && ($count - $doneTasks <= $this->doneTasksRows));
            if ($row->getExtra('error', 0) || $isVisibleRow) {
                $table->addRow([
                    $this->formatTitle($rowTitle, $row),
                    number_format($row->getCount()),
                    number_format($row->getExtra('success', 0)),
                    number_format($row->getExtra('skip', 0)),
                    number_format($row->getExtra('error', 0)),
                    number_format($row->getCodeErrorsCount()),
                    $this->progress($row->getProgress()),
                    TimeHelper::formatTime($row->getDuration()),
                    $this->formatMemory($row, $avgMemoryUsage),
                    $rowMessage
                ]);
            }

            $total['count'] += $row->getCount();
            $total['success'] += $row->getExtra('success', 0);
            $total['skip'] += $row->getExtra('skip', 0);
            $total['error'] += $row->getExtra('error', 0);
            $total['code_errors'] += $row->getCodeErrorsCount();
            $total['duration'] += $row->getDuration();

            $doneTasks++;
        }

        if ($count) {
            $table->addRow(new TableSeparator());
        }
    }

    private function progress(float $percent): string
    {
        $width = 20;
        $percent = min($percent, 100);
        $fullBlocks = floor($percent / 100 * $width);
        $partialBlock = round(fmod($percent / 100 * $width, 1) * $width);

        $chars = ['▘', '▌', '▛', '█'];
        return "<fg=green>" .
            str_repeat($chars[3], $fullBlocks) .
            str_repeat($chars[2], $partialBlock >= .66 ? 1 : 0) .
            str_repeat($chars[1], ($partialBlock >= .33 && $partialBlock < .66) ? 1 : 0) .
            str_repeat($chars[0], ($partialBlock > 0 && $partialBlock < .33) ? 1 : 0) .
            '</>' .
            str_repeat('·', $width - $fullBlocks - ($partialBlock > 0 ? 1 : 0)) .
            str_pad(number_format($percent), 5, ' ', STR_PAD_LEFT) . '%';
    }

    /**
     * @param string $tag
     * @param string $data
     * @return string
     */
    private function tag(string $tag, string $data): string
    {
        return '<' . $tag . '>' . $data . '</' . $tag . '>';
    }

    /**
     * @param string $rowTitle
     * @param TaskData $row
     * @return string
     */
    private function formatTitle(string $rowTitle, TaskData $row): string
    {
        if (!$row->getStackedTask()->isInStatus(StackedTask::STATUS_DONE)) {
            return $rowTitle;
        }

        if ($row->getExtra('error', 0) != 0) {
            return $this->tag('red', "\xE2\x9C\x96 " . $rowTitle);
        }

        if ($row->getCodeErrorsCount() != 0) {
            return $this->tag('yellow', "\xE2\x9C\x96 " . $rowTitle);
        }

        if ($row->getExtra('success', 0) + $row->getExtra('skip', 0) == $row->getCount()) {
            return $this->tag('green', "\xE2\x9C\x94 " . $rowTitle);
        }

        return $rowTitle;
    }

    /**
     * @param TaskData $taskData
     * @param int $maxMemory
     * @return string
     */
    private function formatMemory(TaskData $taskData, int $maxMemory): string
    {
        $memoryIndex = $taskData->getMemoryPeak() / $maxMemory;
        $text = DataHelper::convertBytes($taskData->getMemoryUsage()) . '/' . DataHelper::convertBytes($taskData->getMemoryPeak());

        if ($memoryIndex > 3) {
            return "<red>$text</red>";
        } elseif ($memoryIndex > 2) {
            return "<yellow>$text</yellow>";
        }

        return "$text";
    }

    /**
     * @param TaskData[] $data
     * @return int
     */
    private function getAvgMemoryUsage(array $data): int
    {
        $memory = 0;
        $count = 0;
        foreach ($data as $taskData) {
            $memory += $taskData->getMemoryPeak();
            $count++;
        }

        if ($count === 0) {
            return 0;
        }

        return floor($memory / $count);
    }
}
