<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.40" styleCategories="Symbology|Labeling|Rendering"
      simplifyLocal="1" simplifyDrawingTol="1" simplifyMaxScale="1"
      simplifyDrawingHints="1" simplifyAlgorithm="0"
      labelsEnabled="0">

  <renderer-v2 type="singleSymbol" symbollevels="0" enableorderby="0">
    <symbols>
<symbol name="0" type="line" clip_to_extent="1" force_rhr="0" alpha="1">
  <data_defined_properties>
    <Option type="Map"><Option name="name" value="" type="QString"/>
    <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
  </data_defined_properties>
  <!-- Base line (black) -->
  <layer class="SimpleLine" pass="0" locked="0" enabled="1">
    <Option type="Map">
      <Option name="line_color" value="50,50,50,255" type="QString"/>
      <Option name="line_width" value="0.7" type="QString"/>
      <Option name="line_width_unit" value="MM" type="QString"/>
      <Option name="capstyle" value="flat" type="QString"/>
      <Option name="joinstyle" value="miter" type="QString"/>
      <Option name="line_style" value="solid" type="QString"/>
      <Option name="use_custom_dash" value="0" type="QString"/>
      <Option name="offset" value="0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
    </Option>
  </layer>
  <!-- Perpendicular ticks (white, LK50 Bahn symbol) -->
  <layer class="HashLine" pass="0" locked="0" enabled="1">
    <Option type="Map">
      <Option name="hash_length" value="2" type="QString"/>
      <Option name="hash_length_unit" value="MM" type="QString"/>
      <Option name="interval" value="4" type="QString"/>
      <Option name="interval_unit" value="MM" type="QString"/>
      <Option name="offset" value="0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="rotate" value="1" type="QString"/>
    </Option>
    <symbol name="@0@1" type="line" clip_to_extent="1" force_rhr="0" alpha="1">
      <data_defined_properties>
        <Option type="Map"><Option name="name" value="" type="QString"/>
        <Option name="properties" type="Map"/><Option name="type" value="collection" type="QString"/></Option>
      </data_defined_properties>
      <layer class="SimpleLine" pass="0" locked="0" enabled="1">
        <Option type="Map">
          <Option name="line_color" value="255,255,255,255" type="QString"/>
          <Option name="line_width" value="0.4" type="QString"/>
          <Option name="line_width_unit" value="MM" type="QString"/>
          <Option name="capstyle" value="flat" type="QString"/>
          <Option name="joinstyle" value="miter" type="QString"/>
          <Option name="line_style" value="solid" type="QString"/>
          <Option name="use_custom_dash" value="0" type="QString"/>
          <Option name="offset" value="0" type="QString"/>
          <Option name="offset_unit" value="MM" type="QString"/>
        </Option>
      </layer>
    </symbol>
  </layer>
</symbol>

    </symbols>
  </renderer-v2>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
</qgis>
