<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.40" styleCategories="Symbology|Labeling|Rendering"
      simplifyLocal="1" simplifyDrawingTol="1" simplifyMaxScale="1"
      simplifyDrawingHints="1" simplifyAlgorithm="0"
      labelsEnabled="1">

  <renderer-v2 type="singleSymbol" symbollevels="0" enableorderby="0">
    <symbols>
<symbol name="0" type="fill" clip_to_extent="1" force_rhr="0" alpha="1">
  <data_defined_properties>
    <Option type="Map"><Option name="name" value="" type="QString"/>
    <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
  </data_defined_properties>
  <layer class="SimpleFill" pass="0" locked="0" enabled="1">
    <Option type="Map">
      <Option name="color" value="240,220,210,140" type="QString"/>
      <Option name="style" value="solid" type="QString"/>
      <Option name="outline_color" value="180,140,120,255" type="QString"/>
      <Option name="outline_width" value="0.4" type="QString"/>
      <Option name="outline_width_unit" value="MM" type="QString"/>
      <Option name="outline_style" value="solid" type="QString"/>
      <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="use_custom_dash" value="0" type="QString"/>

      <Option name="joinstyle" value="round" type="QString"/>
      <Option name="offset" value="0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
    </Option>
  </layer>
</symbol>

    </symbols>
  </renderer-v2>
<labeling type="simple">
  <settings calloutType="simple">
    <text-style fieldName="aname" fontSize="9"
                fontFamily="Noto Sans" fontWeight="50"
                textColor="0,0,0,255" textOpacity="1"
                namedStyle="Regular" isExpression="0"
                multilineHeight="1" blendMode="0"
                previewBkgrdColor="255,255,255,255"
                allowHtml="0" forcedBold="0" forcedItalic="0"
                capitalization="0" useSubstitutions="0">
      <text-buffer bufferSize="0.8" bufferSizeUnits="MM"
                   bufferColor="255,255,255,200" bufferDraw="1"
                   bufferBlendMode="0" bufferNoFill="0"
                   bufferOpacity="0.8" bufferJoinStyle="round"/>
    </text-style>
    <placement priority="5" maxCurvedCharAngleIn="25"
               maxCurvedCharAngleOut="-25"
               dist="1.5" distUnits="MM"
               placement="2" placementFlags="10"
               repeatDistance="150" repeatDistanceUnits="MM"
               overrunDistance="0" overrunDistanceUnit="MM"
               centroidWhole="0" centroidInside="0"
               offsetType="0" geometryGenerator=""
               geometryGeneratorType="PointGeometry"
               geometryGeneratorEnabled="0"
               labelOffsetMapUnitScale="3x:0,0,0,0,0,0"
               xOffset="0" yOffset="0" offsetUnits="MM"
               rotationAngle="0" rotationUnit="AngleDegrees"
               preserveRotation="1" overlapHandling="PreventOverlap"
               allowDegraded="0" lineAnchorType="0"
               lineAnchorPercent="0.5"
               lineAnchorClipping="0"/>
    <rendering scaleMin="0" scaleMax="100000"
               scaleVisibility="1" limitNumLabels="0"
               obstacle="1" obstacleFactor="1"
               obstacleType="1" zIndex="0"
               drawUnplaced="0" unplacedVisibility="0"
               fontMinPixelSize="3" fontMaxPixelSize="10000"
               fontLimitPixelSize="0" minFeatureSize="0"
               mergeLines="1" upsidedownLabels="0"
               displayAll="0"/>
  </settings>
</labeling>

  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
</qgis>
