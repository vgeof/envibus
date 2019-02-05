%#template to generate a HTML table from a list of tuples (or list of lists, or tuple of tuples or ...)
<p>Predictions:</p>
<table border="1" style="width:100%">
<tr>
%for row in desc:
<th>{{row}}</th>
%end
</tr>
%for row in rows:
<tr>
  %for col in row:
    <td>{{col}}</td>
  %end
</tr>
%end
</table>
