<ul>
<b>Описание проекта</b>
<li> AdminUser создает топики, отправляет сгенерированные запрещенные слова и данные по пользователям.</li>
<li> ProcessMessageCenter обрабатывает сообщения пользователей, модифицирует их при наличии запрещенных слов, блокирует, если отправитель находится в черном списке получателя.</li>
<li> User отправляет и получает сообщения.</li>
</ul>

<b>Инструкция по запуску проекта</b> 
1) Запустить докер файл.
2) Запустить сервисы в следующем порядке: 
   -> AdminUser (дождаться создания топиков, отправки слов и данных по пользователям); 
   -> ProcessMessageCenter (дождаться логов по обновлению информации по юзерам);
   -> User.
