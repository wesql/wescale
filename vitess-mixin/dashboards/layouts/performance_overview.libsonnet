local helpers = import '../resources/grafonnet/helpers/helpers.libsonnet';
local panels = import '../resources/grafonnet/panels.libsonnet';
local rows = import '../resources/grafonnet/rows.libsonnet';
local singlestats = import '../resources/grafonnet/singlestats.libsonnet';
local templates = import '../resources/grafonnet/templates.libsonnet';
local texts = import '../resources/grafonnet/texts.libsonnet';
local heatmaps = import '../resources/grafonnet/heatmaps.libsonnet';

local config = import '../../config.libsonnet';
local rows_helper = helpers.default;

{
  grafanaDashboards+:: {
    'performance_overview.json':
      helpers.dashboard.getDashboard(config._config.grafanaDashboardMetadata.performanceOverview)
      .addTemplates([
        templates.interval,
        templates.hostVttablet,
        templates.vtgatehost,
        templates.table,
      ])
      .addLink(helpers.default.getDashboardLink(config._config.dashborardLinks))
      .addPanels([
        # summary row (y: 0)
        rows.summary { gridPos: { h: 1, w: 24, x: 0, y: 0 } },

        # vtgate & vttablet summary panels (y: 1)
        helpers.vtgate.getSingleStat(config.vtgate.singlestats.vtgateQPS) { gridPos: { h: 4, w: 4, x: 0, y: 1 } },
        singlestats.vtgateSuccessRate { gridPos: { h: 4, w: 4, x: 4, y: 1 } },
        helpers.vtgate.getSingleStat(config.vtgate.singlestats.vtgateQueryLatencyP99) { gridPos: { h: 4, w: 4, x: 8, y: 1 } },

        # Requests VTGate row (y: 5)
        rows.RequestsVTGate { gridPos: { h: 1, w: 24, x: 0, y: 5 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByInstance2) { gridPos: { h: 7, w: 8, x: 0, y: 6 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByDBType) { gridPos: { h: 7, w: 8, x: 8, y: 6 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByKeyspace) { gridPos: { h: 7, w: 8, x: 16, y: 6 } },

        # Requests VTTablet row (y: 13)
        rows.RequestsVTTablet { gridPos: { h: 1, w: 24, x: 0, y: 13 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 14 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByPlanType) { gridPos: { h: 7, w: 8, x: 8, y: 14 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByTable) { gridPos: { h: 7, w: 8, x: 16, y: 14 } },

        # Duration row (y: 21)
        rows.duration { gridPos: { h: 1, w: 24, x: 0, y: 21 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationAVG) { gridPos: { h: 7, w: 8, x: 0, y: 22 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP50) { gridPos: { h: 7, w: 8, x: 8, y: 22 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP95) { gridPos: { h: 7, w: 8, x: 16, y: 22 } },

        # VTTablet Transaction Duration row (y: 29)
        rows.DurationVTTabletTransaction { gridPos: { h: 1, w: 24, x: 0, y: 29 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationAvgByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 30 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationP50ByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 30 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationP95ByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 30 } },

        # VTGate Errors row (y: 37)
        rows.ErrorsVTGate { gridPos: { h: 1, w: 24, x: 0, y: 37 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByCode) { gridPos: { h: 7, w: 8, x: 0, y: 38 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByOperation) { gridPos: { h: 7, w: 8, x: 8, y: 38 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByDbtype) { gridPos: { h: 7, w: 8, x: 16, y: 38 } },

        # Query Pool row (y: 45)
        rows.vitessQueryPool { gridPos: { h: 1, w: 24, x: 0, y: 45 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolAvailableConnections) { gridPos: { h: 7, w: 8, x: 0, y: 46 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolActiveConnections) { gridPos: { h: 7, w: 8, x: 8, y: 46 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolIddleClosedRate) { gridPos: { h: 7, w: 8, x: 16, y: 46 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolWaitCount) { gridPos: { h: 7, w: 8, x: 0, y: 53 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolAvgWaitTime) { gridPos: { h: 7, w: 8, x: 8, y: 53 } },

        # Transaction Pool row (y: 60)
        rows.vitessTransactionPool { gridPos: { h: 1, w: 24, x: 0, y: 60 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolAvailableConnections) { gridPos: { h: 7, w: 8, x: 0, y: 61 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolActiveConnections) { gridPos: { h: 7, w: 8, x: 8, y: 61 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolIddleClosedRate) { gridPos: { h: 7, w: 8, x: 16, y: 61 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolWaitCount) { gridPos: { h: 7, w: 8, x: 0, y: 68 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolAvgWaitTime) { gridPos: { h: 7, w: 8, x: 8, y: 68 } },

        # Vitess Timings row (y: 75)
        rows_helper.getRow(config.row.vitessTimings) { gridPos: { h: 1, w: 24, x: 0, y: 75 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vtgateToVtTabletCallTimeAvgFilteredByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 76 } },
        heatmaps.vttabletQueryTimeDistribution { gridPos: { h: 7, w: 16, x: 8, y: 76 } },

        # Rows Returned row (y: 91)
        rows.rowsReturned { gridPos: { h: 1, w: 24, x: 0, y: 91 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByTableFilteredByInstance) { gridPos: { h: 7, w: 12, x: 0, y: 92 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByPlansFilterByInstance) { gridPos: { h: 7, w: 12, x: 12, y: 92 } },

        # GC row (y: 99)
        rows_helper.getRow(config.row.gc) { gridPos: { h: 1, w: 24, x: 0, y: 99 } },
        helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionCount) { gridPos: { h: 7, w: 8, x: 0, y: 100 } },
        helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionDuration) { gridPos: { h: 7, w: 8, x: 8, y: 100 } },
        helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionDurationQuantiles) { gridPos: { h: 7, w: 8, x: 16, y: 100 } },
        helpers.os.getPanel(config.vtgate.panels.vtgateGarbageCollectionCount) { gridPos: { h: 7, w: 8, x: 0, y: 107 } },
        helpers.os.getPanel(config.vtgate.panels.vtgateGarbageCollectionDuration) { gridPos: { h: 7, w: 8, x: 8, y: 107 } },
        helpers.os.getPanel(config.vtgate.panels.vtgateGarbageCollectionDurationQuantiles) { gridPos: { h: 7, w: 8, x: 16, y: 107 } },
      ]),
  },
}

//todo y coordinate may not be right. row can't be collapsed