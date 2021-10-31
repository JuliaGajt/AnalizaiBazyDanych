
W przeprowadzonej analizie wykorzystano dane pozyskane z Global Historical Climatology Network.

Głównym celem podczas przetważania rekordów było usunięcie zbędnych separatorów (liter i nadmiernych spacji) z kolumn przeznaczonych dla temperatur i opadów. Ułatwiło to zdecydowanie odczyt i analizę informacji. Kolejnym zadaniem było uporządkowanie danych. W moim rozwiązaniu tabela była podzielona na cztery główne kolumny: stacja pogodowa, data, rodzaj zmiennej (maksymalna lub minimalna temperatura lub opady) oraz ich wartość. W tym celu użyto szereu poleceń, które wraz z opisem są dostępne w pliku Command Files/"tidying_data.ipynb".

Dodatkowo podczas obróbki usunięto każdy wiersz posiadający nieznaną wartość. Warto o nim wspomnieć ponieważ tego typu przypadków było dość dużo i utrudniało to analizę.
