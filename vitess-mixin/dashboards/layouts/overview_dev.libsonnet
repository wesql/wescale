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
    'overview_dev.json':
      
      helpers.dashboard.getDashboard(config._config.grafanaDashboardMetadata.devOverview)
      .addTemplates([
        templates.interval,
        templates.hostVttablet,
      ])
      .addLink(helpers.default.getDashboardLink(config._config.dashborardLinks))
      .addPanels([
        # summary
        rows.summary { gridPos: { h: 1, w: 24, x: 0, y: 0 } },
        singlestats.vtgateSuccessRate { gridPos: { h: 4, w: 4, x: 0, y: 1 } },
        singlestats.vttabletQuerySuccess { gridPos: { h: 4, w: 4, x: 4, y: 1 } },
        helpers.vtgate.getSingleStat(config.vtgate.singlestats.vtgateQueryLatencyP99) { gridPos: { h: 4, w: 4, x: 8, y: 1 } },
        helpers.vtgate.getSingleStat(config.vtgate.singlestats.vtgateQPS) { gridPos: { h: 2, w: 4, x: 12, y: 3 } },
        helpers.vttablet.getSingleStat(config.vttablet.singlestats.vttabletQPS) { gridPos: { h: 2, w: 4, x: 12, y: 3 } },
        singlestats.vtgateUp { gridPos: { h: 2, w: 2, x: 16, y: 1 } },
        singlestats.vttabletUp { gridPos: { h: 2, w: 2, x: 16, y: 3 } },
        singlestats.keyspaceCount { gridPos: { h: 2, w: 2, x: 18, y: 1 } },
        singlestats.shardCount { gridPos: { h: 2, w: 2, x: 18, y: 3 } },

        # hostview
        # vtgate RED
        rows.REDVtgate { gridPos: { h: 1, w: 24, x: 0, y: 7 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequests) { gridPos: { h: 6, w: 8, x: 0, y: 8 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRate) { gridPos: { h: 6, w: 8, x: 8, y: 8 } },
        helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99) { gridPos: { h: 6, w: 8, x: 16, y: 8 } },

        # vtgate RED by keyspace
        rows.REDByKeyspace.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByKeyspace) { gridPos: { h: 8, w: 8, x: 0, y: 15 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRateByKeyspace) { gridPos: { h: 8, w: 8, x: 8, y: 15 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99ByKeyspace) { gridPos: { h: 8, w: 8, x: 16, y: 15 } },
        ]){ gridPos: { h: 1, w: 24, x: 0, y: 14 } },

        # vtgate RED by tablet type
        rows.REDByTabletType.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateRequestsByDBType) { gridPos: { h: 7, w: 8, x: 0, y: 24 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorRateByDBType) { gridPos: { h: 7, w: 8, x: 8, y: 24 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP99ByDBType) { gridPos: { h: 7, w: 8, x: 16, y: 24 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 23 } },

        # vttablet RED 
        rows.REDTablet { gridPos: { h: 1, w: 24, x: 0, y: 31 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 32 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorRateByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 32 } },
        helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP99ByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 32 } },

        # vttablet RED by plan type
        rows.REDByPlanType.addPanels([
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByPlanType) { gridPos: { h: 7, w: 8, x: 0, y: 40 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorRateByPlanFilteredByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 40 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP99ByPlan) { gridPos: { h: 7, w: 8, x: 16, y: 40 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 39 } },

        # vttablet RED by table
        rows.REDByTable.addPanels([
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletRequestsByTableFilteredByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 50 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletErrorRateByTableFilteredByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 50 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryDurationP99ByTableFilteredByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 50 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 47 } },

        # vtgate errors 
        rows.errors.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByCode) { gridPos: { h: 7, w: 8, x: 0, y: 58 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByOperation) { gridPos: { h: 7, w: 8, x: 8, y: 58 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateErrorsByDbtype) { gridPos: { h: 7, w: 8, x: 16, y: 58 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 57 } },

        # vtgate duration
        rows.duration.addPanels([
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationAVG) { gridPos: { h: 7, w: 8, x: 0, y: 66 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP50) { gridPos: { h: 7, w: 8, x: 8, y: 66 } },
          helpers.vtgate.getPanel(config.vtgate.panels.vtgateDurationP95) { gridPos: { h: 7, w: 8, x: 16, y: 66 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 65 } },

        # vttablet row returned
        rows.rowsReturned.addPanels([
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByTableFilteredByInstance) { gridPos: { h: 7, w: 12, x: 0, y: 74 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletRowsReturnedByPlansFilterByInstance) { gridPos: { h: 7, w: 12, x: 12, y: 74 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 73 } },

        # vttablet queries/errors
        rows_helper.getRow(config.row.queryErrors).addPanels([
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueriesKilled) { gridPos: { h: 7, w: 8, x: 0, y: 82 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryErrorsByType) { gridPos: { h: 7, w: 8, x: 8, y: 82 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 81 } },

        rows.vitessQueryPool.addPanels([
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolAvailableConnections) { gridPos: { h: 7, w: 8, x: 0, y: 90 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolActiveConnections) { gridPos: { h: 7, w: 8, x: 8, y: 90 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolIddleClosedRate) { gridPos: { h: 7, w: 8, x: 16, y: 90 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolWaitCount) { gridPos: { h: 7, w: 8, x: 0, y: 97 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletQueryPoolAvgWaitTime) { gridPos: { h: 7, w: 8, x: 8, y: 97 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 89 } },

        rows.vitessTransactionPool.addPanels([
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolAvailableConnections) { gridPos: { h: 7, w: 8, x: 0, y: 105 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolActiveConnections) { gridPos: { h: 7, w: 8, x: 8, y: 105 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolIddleClosedRate) { gridPos: { h: 7, w: 8, x: 16, y: 105 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolWaitCount) { gridPos: { h: 7, w: 8, x: 0, y: 112 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionPoolAvgWaitTime) { gridPos: { h: 7, w: 8, x: 8, y: 112 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 104 } },

        rows_helper.getRow(config.row.vitessTimings).addPanels([
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationAvgByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 120 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationP50ByInstance) { gridPos: { h: 7, w: 8, x: 8, y: 120 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletTransactionDurationP95ByInstance) { gridPos: { h: 7, w: 8, x: 16, y: 120 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vtgateToVtTabletCallTimeAvgFilteredByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 127 } },
          heatmaps.vttabletQueryTimeDistribution { gridPos: { h: 7, w: 16, x: 8, y: 127 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 119 } },

        # TODO vttablet slow queries for dev env

        rows_helper.getRow(config.row.mysqlTimings).addPanels([
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletMysqlTimeAvgFilteredByInstance) { gridPos: { h: 7, w: 8, x: 0, y: 142 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletMysqlExecTimeP50FilterebyInstance) { gridPos: { h: 7, w: 8, x: 8, y: 142 } },
          helpers.vttablet.getPanel(config.vttablet.panels.vttabletMysqlExecTimeP95FilterebyInstance) { gridPos: { h: 7, w: 8, x: 16, y: 142 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 141 } },

        # GC Misc
        rows_helper.getRow(config.row.misc).addPanels([
          helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionCount) { gridPos: { h: 7, w: 8, x: 0, y: 150 } },
          helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionDuration) { gridPos: { h: 7, w: 8, x: 8, y: 150 } },
          helpers.os.getPanel(config.vttablet.panels.vttabletGarbageCollectionDurationQuantiles) { gridPos: { h: 7, w: 8, x: 16, y: 150 } },
          helpers.os.getPanel(config.vtgate.panels.vtgateGarbageCollectionCount) { gridPos: { h: 7, w: 8, x: 0, y: 157 } },
          helpers.os.getPanel(config.vtgate.panels.vtgateGarbageCollectionDuration) { gridPos: { h: 7, w: 8, x: 8, y: 157 } },
          helpers.os.getPanel(config.vtgate.panels.vtgateGarbageCollectionDurationQuantiles) { gridPos: { h: 7, w: 8, x: 16, y: 157 } },
        ]) { gridPos: { h: 1, w: 24, x: 0, y: 149 } },

      ]),

  },
}