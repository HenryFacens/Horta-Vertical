<?php
//date('Y-m-d H:i:s');

$data->dia = date('d');
$data->mes = date('m');
$data->ano = date('Y');
$data->hora = date('H');
$data->minuto = date('i');
$data->segundo = date('s');

$data = json_encode($data);

echo $data;



?>