The program should do the following:

1. Create 50 zip-files, each of them contains 100 xml-files with random data and following structure:

&lt;root&gt;

&lt;var name=’id’ value=’&lt;random string&gt;’/&gt;

&lt;var name=’level’ value=’&lt;random integer from 1 to 100&gt;’/&gt;

&lt;objects&gt;

&lt;object name=’&lt;random string&gt;’/&gt;

&lt;object name=’&lt;random string&gt;’/&gt;

…

&lt;/objects&gt;

&lt;/root&gt;

Tag objects contains random number (from 1 to 10) object tag.

2. Process folder with zip-files, parse xml-files and create csv-files:

1st csv: id, level - one row per each xml-file

2nd csv: id, object_name - one row for each tag object


Task 2 must efficiently use multiple CPUs


Задача.
 

Написать программу на Python, которая делает следующие действия:

1. Создает 50 zip-архивов, в каждом 100 xml файлов со случайными данными следующей структуры:

&lt;root&gt;

&lt;var name=’id’ value=’&lt;случайное уникальное строковое значение&gt;’/&gt;

&lt;var name=’level’ value=’&lt;случайное число от 1 до 100&gt;’/&gt;

&lt;objects&gt;

&lt;object name=’&lt;случайное строковое значение&gt;’/&gt;

&lt;object name=’&lt;случайное строковое значение&gt;’/&gt;

…

&lt;/objects&gt;

&lt;/root&gt;

В тэге objects случайное число (от 1 до 10) вложенных тэгов object.

2. Обрабатывает директорию с полученными zip архивами, разбирает вложенные xml файлы и

формирует 2 csv файла:

Первый: id, level - по одной строке на каждый xml файл

Второй: id, object_name - по отдельной строке для каждого тэга object (получится от 1 до 10 строк на

каждый xml файл)

Очень желательно сделать так, чтобы задание 2 эффективно использовало ресурсы многоядерного

процессора. 

Также желательно чтобы программа работала быстро.