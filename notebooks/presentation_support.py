import folium


def folium_del_legend(choropleth: folium.Choropleth, idx: int) -> folium.Choropleth:
    """A hack to remove choropleth legends
    for a first choropleth layer in a loop with index 0.

    The choropleth color-scaled legend sometimes looks too crowded. Until there is an
    option to disable the legend, use this routine to remove any color map children
    from the choropleth.

    Args:
      choropleth: Choropleth objected created by `folium.Choropleth()`

    Returns:
      The same object `choropleth` with any child whose name starts with
      'color_map' removed.
    """
    if idx == 0:
        pass
    else:
        del_list = []
        for child in choropleth._children:
            if child.startswith("color_map"):
                del_list.append(child)
        for del_item in del_list:
            choropleth._children.pop(del_item)
    return choropleth
